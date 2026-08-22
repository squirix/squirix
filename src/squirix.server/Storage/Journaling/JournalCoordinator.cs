using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer pipelined journal coordinator with binary frames (see docs/journal-binary-format.md).</summary>
internal sealed class JournalCoordinator : IJournalCoordinator, IJournalCoordinatorAppendState, IJournalCoordinatorState, IJournalCoordinatorSnapshotState
{
    private const int RingCapacity = 4096;

    private static readonly ParameterizedThreadStart RunEventLoopCallback = static state =>
    {
        if (state is JournalEventLoop eventLoop)
            eventLoop.Run();
    };

    private readonly JournalCoordinatorAppendPipeline _appendPipeline;

    private readonly IJournalSegmentWriter _segmentWriter;
    private readonly VolatileDouble _appendLatency = new();
    private readonly VolatileField<Exception> _flushLoopFailure = new();
    private long _bytes;
    private int _disposed;
    private ulong _nextSequence;
    private long _ops;

    internal JournalCoordinator(PersistenceOptions opt, State manifest, Ledger manifestStore, JournalStartupGate startupGate)
    {
        Options = opt;
        Ledger = manifestStore;
        StartupGate = startupGate;
        _segmentWriter = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        _appendPipeline = new JournalCoordinatorAppendPipeline(this);
        DurabilityPipeline = new JournalCoordinatorDurabilityPipeline(this, this);
        var bridge = new JournalEventLoopBridge(this, DurabilityPipeline);
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Options.DataDir);
        var currentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var eventLoopStartup = new JournalEventLoopStartup(currentSegmentIndex, totalBytes, segmentCount);
        EventLoop = new JournalEventLoop(bridge, Ring, _segmentWriter, Options, eventLoopStartup, BackgroundCancellation.Token);
        GroupCommit = Options.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(EventLoop.FlushGroupCommitOnJournalThread, () => Ring.NotifyWorkAvailable(), Options)
            : null;
        EventLoop.AttachGroupCommit(GroupCommit);
        _ = DirectoryEx.CreateDirectory(Options.DataDir);
        _nextSequence = JournalRecoveryScan.DetermineNextSequence(manifest, Options);
        JournalThread = new Thread(RunEventLoopCallback)
        {
            IsBackground = true,
            Name = "squirix-journal-io",
        };
        JournalThread.Start(EventLoop);
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex => EventLoop.CurrentSegmentIndex;

    public bool HasFlushLoopFailure => _flushLoopFailure.Read() != null;

    public long HighWaterBytes => EventLoop.Policy.HighWaterBytes;

    public bool IsJournalGroupCommitEnabled => Options.IsJournalGroupCommitEnabled;

    public long MaxBytes => EventLoop.Policy.MaxTotalBytes;

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public double RecentAppendLatencyMs => _appendLatency.Read();

    public long UsedBytes => EventLoop.JournalTotalBytes;

    public CancellationTokenSource BackgroundCancellation { get; } = new();

    public MutableInt32 DurabilityFlushScheduledFlag { get; } = new();

    public DurabilityAckRegistry DurabilityAcks { get; } = new();

    public JournalEventLoop EventLoop { get; }

    public JournalDurabilityGroupCommit? GroupCommit { get; }

    public Thread JournalThread { get; }

    public Ledger Ledger { get; }

    public SemaphoreSlim MutationGate { get; } = new(1, 1);

    public PersistenceOptions Options { get; }

    public BoundedJournalRing Ring { get; } = new(RingCapacity);

    public JournalCoordinatorDurabilityPipeline DurabilityPipeline { get; }

    public MutableInt32 QueuedAppendsCounter { get; } = new();

    public QuiescenceGate InFlightApplyGate { get; } = new();

    public JournalStartupGate StartupGate { get; }

    internal long ActiveSegmentWrittenBytes => EventLoop.ActiveSegmentWrittenBytes;

    public Exception? GetJournalThreadFailure() => _flushLoopFailure.Read();

    public void SetJournalThreadFailure(Exception? value) => _flushLoopFailure.Write(value);

    public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        var record = _appendPipeline.AllocateIdempotencyRecord(operationId, fingerprint, responseBytes);
        return _appendPipeline.AppendRecordCoreAsync(record, cancellationToken);
    }

    public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes.Span);
        if (Options.IsJournalGroupCommitEnabled)
            return _appendPipeline.AppendPutAndAwaitDurabilityViaGroupCommitAsync(key, entryBytes, cancellationToken);

        return _appendPipeline.AppendRecordWithDurabilityCoreAsync(_appendPipeline.AllocateRecord(key, JournalOperationKind.Put, entryBytes), cancellationToken);
    }

    public ValueTask AppendPutAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes.Span);
        return _appendPipeline.AppendRecordCoreAsync(_appendPipeline.AllocateRecord(key, JournalOperationKind.Put, entryBytes), cancellationToken);
    }

    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => _appendPipeline.AppendRecordCoreAsync(
        _appendPipeline.AllocateRecord(key, JournalOperationKind.Remove),
        cancellationToken);

    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => _appendPipeline.AppendRecordCoreAsync(
        _appendPipeline.AllocateRecord(key, JournalOperationKind.RemoveExpiration),
        cancellationToken);

    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => _appendPipeline.AppendRecordCoreAsync(
        _appendPipeline.AllocateRecord(key, JournalOperationKind.TouchExpiration, touchExpirationUtc: expiresUtc),
        cancellationToken);

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        DurabilityPipeline.ThrowIfJournalThreadFailed();
        return GroupCommit?.AwaitCommitAsync(cancellationToken) ?? DurabilityPipeline.FlushAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var failures = new List<Exception>();

        // The shutdown marker must enter the ring BEFORE background cancellation is requested: the
        // journal thread dequeues FIFO, so every item enqueued before it is drained and written, and
        // only then does the thread observe Shutdown and exit. Cancelling first would let the thread
        // exit via OperationCanceledException while frames were still queued, silently dropping them.
        await DurabilityPipeline.EnqueueShutdownAsync().ConfigureAwait(false);
        await DurabilityPipeline.AwaitJournalThreadDuringDisposeAsync(failures).ConfigureAwait(false);

        try
        {
            await BackgroundCancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown can dispose the CTS before cancellation is observed.
        }

        if (GroupCommit != null)
            await GroupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(JournalCoordinator))).ConfigureAwait(false);

        DurabilityPipeline.FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));

        await _segmentWriter.DisposeAsync().ConfigureAwait(false);
        Ring.Dispose();
        BackgroundCancellation.Dispose();
        MutationGate.Dispose();
        JournalCoordinatorDurabilityPipeline.ThrowDisposeFailures(failures);
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        DurabilityPipeline.ThrowIfJournalThreadFailed();
        await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await MutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await DurabilityPipeline.EnqueueMaintenanceAsync(action, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = MutationGate.Release();
        }
    }

    public async ValueTask<TResult> ExecuteSnapshotCutAsync<TState, TBarrier, TResult>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        Func<TState, ulong, TBarrier, CancellationToken, ValueTask<TResult>> buildOutsideBarrier,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(captureUnderBarrier);
        ArgumentNullException.ThrowIfNull(buildOutsideBarrier);
        DurabilityPipeline.ThrowIfJournalThreadFailed();

        await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await DurabilityPipeline.WaitForSnapshotCutAdmissionAsync(cancellationToken).ConfigureAwait(false);
        ulong seqAtFlush;
        TBarrier barrierState;
        try
        {
            await DurabilityPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);
            seqAtFlush = NextSequence > 0 ? NextSequence - 1UL : 0UL;
            barrierState = await captureUnderBarrier(state, seqAtFlush, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = MutationGate.Release();
        }

        return await buildOutsideBarrier(state, seqAtFlush, barrierState, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) =>
        ExecuteUnderSnapshotBarrierAsync(action, static (handler, ct) => handler(ct), cancellationToken);

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        DurabilityPipeline.ThrowIfJournalThreadFailed();

        try
        {
            await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            await MutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            throw new InvalidOperationException("journal coordinator is disposed.", ex);
        }

        try
        {
            return await action(state, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = MutationGate.Release();
        }
    }

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => StartupGate.WaitAsync(cancellationToken);

    ulong IJournalCoordinatorAppendState.AllocateSequence()
    {
        while (true)
        {
            var current = Volatile.Read(ref _nextSequence);
            var next = current + 1UL;
            if (Interlocked.CompareExchange(ref _nextSequence, next, current) == current)
                return next;
        }
    }

    void IJournalCoordinatorAppendState.RecordAppendMetrics(int frameLength, long startedMs)
    {
        var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
        var currentLatency = _appendLatency.Read();
        _appendLatency.Write(currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));
        _ = Interlocked.Add(ref _bytes, frameLength);
        _ = Interlocked.Increment(ref _ops);
        NotifyAppended();
    }

    private void NotifyAppended() => OnAppended?.Invoke(this, EventArgs.Empty);

    /// <summary>
    /// Forwards <see cref="IJournalEventLoopHost" /> callbacks from <see cref="JournalEventLoop" />
    /// to <see cref="JournalCoordinator" /> without the coordinator implementing the interface directly.
    /// </summary>
    [Immutable]
    private sealed class JournalEventLoopBridge : IJournalEventLoopHost
    {
        private readonly JournalCoordinator _coordinator;
        private readonly JournalCoordinatorDurabilityPipeline _durabilityPipeline;

        internal JournalEventLoopBridge(JournalCoordinator coordinator, JournalCoordinatorDurabilityPipeline durabilityPipeline)
        {
            _coordinator = coordinator;
            _durabilityPipeline = durabilityPipeline;
        }

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _durabilityPipeline.CompleteDurabilityCheckpointOnJournalThread(item);

        void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _coordinator.QueuedAppendsCounter.Value);

        void IJournalEventLoopHost.FailPipeline(Exception reason) => _durabilityPipeline.FailJournalPipeline(reason);

        void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex) => _coordinator.Ledger.EnqueueRoll(
            targetSegmentIndex,
            Volatile.Read(ref _coordinator._nextSequence),
            () => _durabilityPipeline.OnManifestRollSucceeded(),
            ex => _durabilityPipeline.OnManifestRollFailed(ex));

        void IJournalEventLoopHost.SetNextSequence(ulong value) => Volatile.Write(ref _coordinator._nextSequence, value);

        void IJournalEventLoopHost.ThrowIfJournalThreadFailed() => _durabilityPipeline.ThrowIfJournalThreadFailed();
    }

    /// <summary>Append encoding and ring enqueue for a journal coordinator.</summary>
    [Immutable]
    private sealed class JournalCoordinatorAppendPipeline
    {
        private readonly IJournalCoordinatorAppendState _owner;

        internal JournalCoordinatorAppendPipeline(IJournalCoordinatorAppendState owner)
        {
            _owner = owner;
        }

        internal JournalRecord AllocateIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes)
        {
            var record = JournalRecord.RentForAppend();
            record.Sequence = _owner.AllocateSequence();
            record.UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            record.Operation = JournalOperationKind.IdempotencyOutcome;
            record.Key = new CacheKey(string.Empty, string.Empty);
            record.IdempotencyOperationId = operationId;
            record.IdempotencyFingerprint = fingerprint;
            record.IdempotencyResponseBytes = responseBytes;
            return record;
        }

        internal JournalRecord AllocateRecord(CacheKey key, JournalOperationKind operation, ReadOnlyMemory<byte> putEntryBytes = default, DateTime? touchExpirationUtc = null)
        {
            var record = JournalRecord.RentForAppend();
            record.Sequence = _owner.AllocateSequence();
            record.UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            record.Operation = operation;
            record.Key = key;
            record.PutEntryBytes = putEntryBytes;
            record.TouchExpirationUtc = touchExpirationUtc;
            return record;
        }

        internal async ValueTask AppendPutAndAwaitDurabilityViaGroupCommitAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            await AppendRecordCoreAsync(AllocateRecord(key, JournalOperationKind.Put, entryBytes), cancellationToken).ConfigureAwait(false);
            if (_owner.GroupCommit != null)
            {
                await _owner.GroupCommit.AwaitCommitAsync(cancellationToken).ConfigureAwait(false);
                return;
            }

            await _owner.DurabilityPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);
        }

        internal async ValueTask AppendRecordCoreAsync(JournalRecord record, CancellationToken cancellationToken)
        {
            _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
            await _owner.StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var encode = BinaryJournalCodec.PrepareEncode(record);
                var frameLen = JournalFraming.FrameTotalLength(encode.BodyLength);
                var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                _ = BinaryJournalCodec.Encode(record, frameBytes.AsSpan(bodyOffset, encode.BodyLength), in encode);
                JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), frameBytes.AsSpan(bodyOffset, encode.BodyLength));
                var startedMs = Environment.TickCount64;
                await EnqueueAppendAsync(frameBytes, frameLen, cancellationToken).ConfigureAwait(false);
                _owner.RecordAppendMetrics(frameLen, startedMs);
            }
            finally
            {
                record.ReturnToAppendPool();
            }
        }

        internal async ValueTask AppendRecordWithDurabilityCoreAsync(JournalRecord record, CancellationToken cancellationToken)
        {
            _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
            await _owner.StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var encode = BinaryJournalCodec.PrepareEncode(record);
                var frameLen = JournalFraming.FrameTotalLength(encode.BodyLength);
                var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                _ = BinaryJournalCodec.Encode(record, frameBytes.AsSpan(bodyOffset, encode.BodyLength), in encode);
                JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), frameBytes.AsSpan(bodyOffset, encode.BodyLength));
                var startedMs = Environment.TickCount64;
                var ack = DurabilityAck.Rent();
                try
                {
                    var ackWaitTask = ack.AwaitAsync(CancellationToken.None);
                    await EnqueueAppendWithDurabilityAsync(frameBytes, frameLen, ack, cancellationToken).ConfigureAwait(false);
                    await ackWaitTask.ConfigureAwait(false);
                }
                finally
                {
                    ack.ReturnToPool();
                }

                _owner.RecordAppendMetrics(frameLen, startedMs);
            }
            finally
            {
                record.ReturnToAppendPool();
            }
        }

        private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            var appendAck = _owner.Options.IsJournalGroupCommitEnabled ? DurabilityAck.Rent() : null;
            var appendWaitTask = appendAck?.AwaitAsync(CancellationToken.None) ?? default;
            var enqueued = false;
            try
            {
                var item = JournalWorkItem.Append(frameBytes, frameLength, appendAck);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;
                if (appendAck != null)
                {
                    try
                    {
                        await appendWaitTask.ConfigureAwait(false);
                    }
                    finally
                    {
                        appendAck.ReturnToPool();
                    }
                }
            }
            catch when (!enqueued)
            {
                ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                appendAck?.ReturnToPool();
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                throw;
            }
        }

        private async ValueTask EnqueueAppendWithDurabilityAsync(byte[] frameBytes, int frameLength, DurabilityAck ack, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            var enqueued = false;
            try
            {
                var item = JournalWorkItem.AppendWithDurability(ack, frameBytes, frameLength);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;
            }
            catch when (!enqueued)
            {
                ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                throw;
            }
        }
    }
}
