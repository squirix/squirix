using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer pipelined journal coordinator with binary frames (see docs/journal-binary-format.md).</summary>
internal sealed class JournalCoordinator : IJournalCoordinator
{
    private const int RingCapacity = 4096;
    private readonly JournalCoordinatorAppendPipeline _appendPipeline;
    private readonly ManifestRollPublisher _manifestRollPublisher;
    private readonly Lock _pendingMemoryApplyLock = new();

    private readonly IJournalSegmentWriter _segmentWriter;
    private double _avgAppendLatencyMs;
    private long _bytes;
    private int _disposed;
    private Exception? _journalThreadFailure;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;

    [SuppressMessage(
        "NDepend",
        "ND2500:DontCreateThreadsExplicitly",
        Justification = "Single-writer journal event loop requires a dedicated long-lived I/O thread; Task.Run is banned on infrastructure paths.")]
    internal JournalCoordinator(PersistenceOptions opt, State manifest, ManifestStore manifestStore, JournalStartupGate startupGate)
    {
        Options = opt;
        ManifestStore = manifestStore;
        StartupGate = startupGate;
        _segmentWriter = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        _appendPipeline = new JournalCoordinatorAppendPipeline(this);
        DurabilityPipeline = new JournalCoordinatorDurabilityPipeline(this);
        _manifestRollPublisher = new ManifestRollPublisher(manifestStore, ex => DurabilityPipeline.OnManifestRollFailed(ex));
        var bridge = new JournalEventLoopBridge(this, DurabilityPipeline, _manifestRollPublisher);
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Options.DataDir);
        var currentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var eventLoopStartup = new JournalEventLoopStartup(currentSegmentIndex, totalBytes, segmentCount);
        EventLoop = new JournalEventLoop(bridge, Ring, _segmentWriter, Options, eventLoopStartup, BackgroundCancellation.Token);
        GroupCommit = Options.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(EventLoop.FlushGroupCommitOnJournalThread, () => Ring.NotifyWorkAvailable(), Options)
            : null;
        EventLoop.AttachGroupCommit(GroupCommit);
        _ = DirectoryEx.CreateDirectory(Options.DataDir);
        _nextSequence = JournalRecoveryScan.DetermineNextSequence(manifest, Options);
        JournalThread = new Thread(EventLoop.Run) { IsBackground = true, Name = "squirix-journal-io" };
        JournalThread.Start();
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex => EventLoop.CurrentSegmentIndex;

    public long HighWaterBytes => EventLoop.Policy.HighWaterBytes;

    public bool HasFlushLoopFailure => Volatile.Read(ref _journalThreadFailure) is not null;

    public bool IsJournalGroupCommitEnabled => Options.IsJournalGroupCommitEnabled;

    public long MaxBytes => EventLoop.Policy.MaxTotalBytes;

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public double RecentAppendLatencyMs => Volatile.Read(ref _avgAppendLatencyMs);

    public long UsedBytes => EventLoop.JournalTotalBytes;

    internal long ActiveSegmentWrittenBytes => EventLoop.ActiveSegmentWrittenBytes;

    internal CancellationTokenSource BackgroundCancellation { get; } = new();

    internal MutableInt32 DurabilityFlushScheduledFlag { get; } = new();

    internal JournalDurabilityWaiterRegistry DurabilityWaiters { get; } = new();

    internal JournalEventLoop EventLoop { get; }

    internal JournalDurabilityGroupCommit? GroupCommit { get; }

    internal Thread JournalThread { get; }

    internal ref Exception? JournalThreadFailureField => ref _journalThreadFailure;

    internal ManifestStore ManifestStore { get; }

    internal SemaphoreSlim MutationGate { get; } = new(1, 1);

    internal PersistenceOptions Options { get; }

    internal BoundedJournalRing Ring { get; } = new(RingCapacity);

    private ref long AppendedBytesField => ref _bytes;

    private ref long AppendedOpsField => ref _ops;

    private ref double AvgAppendLatencyMsField => ref _avgAppendLatencyMs;

    private JournalCoordinatorDurabilityPipeline DurabilityPipeline { get; }

    private ref ulong NextSequenceField => ref _nextSequence;

    private MutableInt32 QueuedAppendsCounter { get; } = new();

    private JournalStartupGate StartupGate { get; }

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

    public void BeginPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            _pendingMemoryApplyCount++;
    }

    public void CompletePendingMemoryApply()
    {
        TaskCompletionSource? drained = null;
        lock (_pendingMemoryApplyLock)
        {
            if (_pendingMemoryApplyCount <= 0)
                throw new InvalidOperationException("No pending journal memory apply is registered.");

            _pendingMemoryApplyCount--;
            if (_pendingMemoryApplyCount is 0)
            {
                drained = _pendingMemoryApplyDrained;
                _pendingMemoryApplyDrained = null;
            }
        }

        drained?.SetResult();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) is 1)
            return;

        var failures = new List<Exception>();
        try
        {
            await BackgroundCancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown can dispose the CTS before cancellation is observed.
        }

        if (GroupCommit is not null)
            await GroupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(JournalCoordinator))).ConfigureAwait(false);

        DurabilityPipeline.FailPendingDurabilityWaiters(new ObjectDisposedException(nameof(JournalCoordinator)));

        await DurabilityPipeline.EnqueueShutdownAsync().ConfigureAwait(false);
        await DurabilityPipeline.AwaitJournalThreadDuringDisposeAsync(failures).ConfigureAwait(false);
        await _segmentWriter.DisposeAsync().ConfigureAwait(false);
        _manifestRollPublisher.Dispose();
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

    internal bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
    }

    internal ValueTask WaitForPendingMemoryApplyDrainAsync(CancellationToken cancellationToken)
    {
        Task waitTask;
        lock (_pendingMemoryApplyLock)
        {
            if (_pendingMemoryApplyCount is 0)
                return ValueTask.CompletedTask;

            _pendingMemoryApplyDrained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            waitTask = _pendingMemoryApplyDrained.Task;
        }

        return new ValueTask(waitTask.WaitAsync(cancellationToken));
    }

    private void NotifyAppended() => OnAppended?.Invoke(this, EventArgs.Empty);

    /// <summary>Append encoding and ring enqueue for <see cref="JournalCoordinator" />.</summary>
    private sealed class JournalCoordinatorAppendPipeline
    {
        private readonly JournalCoordinator _owner;

        internal JournalCoordinatorAppendPipeline(JournalCoordinator owner)
        {
            _owner = owner;
        }

        internal JournalRecord AllocateIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes)
        {
            var record = JournalRecord.RentForAppend();
            record.Sequence = AllocateSequence();
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
            record.Sequence = AllocateSequence();
            record.UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            record.Operation = operation;
            record.Key = key;
            record.PutEntryBytes = putEntryBytes;
            record.TouchExpirationUtc = touchExpirationUtc;
            return record;
        }

        internal async ValueTask AppendPutAndAwaitDurabilityViaGroupCommitAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
        {
            await _owner.AppendPutAsync(key, entryBytes, cancellationToken).ConfigureAwait(false);
            await _owner.AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
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
                RecordAppendMetrics(frameLen, startedMs);
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
                var waiter = JournalDurabilityWaiter.Rent();
                try
                {
                    var waitTask = waiter.AwaitAsync(CancellationToken.None);
                    await EnqueueAppendWithDurabilityAsync(frameBytes, frameLen, waiter, cancellationToken).ConfigureAwait(false);
                    await waitTask.ConfigureAwait(false);
                }
                finally
                {
                    waiter.ReturnToPool();
                }

                RecordAppendMetrics(frameLen, startedMs);
            }
            finally
            {
                record.ReturnToAppendPool();
            }
        }

        private ulong AllocateSequence()
        {
            while (true)
            {
                var current = Volatile.Read(ref _owner.NextSequenceField);
                var next = current + 1UL;
                if (Interlocked.CompareExchange(ref _owner.NextSequenceField, next, current) == current)
                    return next;
            }
        }

        private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);

            var appendCompleted = _owner.Options.IsJournalGroupCommitEnabled ? JournalDurabilityWaiter.Rent() : null;
            var appendWaitTask = appendCompleted?.AwaitAsync(CancellationToken.None) ?? default;
            var enqueued = false;

            try
            {
                var item = new JournalWorkItem(JournalWorkKind.Append, appendCompleted, frameBytes: frameBytes, frameLength: frameLength);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;

                if (appendCompleted is not null)
                {
                    await appendWaitTask.ConfigureAwait(false);
                    appendCompleted.ReturnToPool();
                }
            }
            catch when (!enqueued)
            {
                appendCompleted?.ReturnToPool();
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                throw;
            }
        }

        private async ValueTask EnqueueAppendWithDurabilityAsync(byte[] frameBytes, int frameLength, JournalDurabilityWaiter durabilityWaiter, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            var enqueued = false;
            try
            {
                var item = new JournalWorkItem(JournalWorkKind.AppendWithDurability, durabilityWaiter: durabilityWaiter, frameBytes: frameBytes, frameLength: frameLength);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;
            }
            catch when (!enqueued)
            {
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                throw;
            }
        }

        private void RecordAppendMetrics(int frameLen, long startedMs)
        {
            var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
            var currentLatency = Volatile.Read(ref _owner.AvgAppendLatencyMsField);
            Volatile.Write(ref _owner.AvgAppendLatencyMsField, currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));

            _ = Interlocked.Add(ref _owner.AppendedBytesField, frameLen);
            _ = Interlocked.Increment(ref _owner.AppendedOpsField);
            _owner.NotifyAppended();
        }
    }

    /// <summary>
    /// Forwards <see cref="IJournalEventLoopHost" /> callbacks from <see cref="JournalEventLoop" />
    /// to <see cref="JournalCoordinator" /> without the coordinator implementing the interface directly.
    /// </summary>
    private sealed class JournalEventLoopBridge : IJournalEventLoopHost
    {
        private readonly JournalCoordinator _coordinator;
        private readonly JournalCoordinatorDurabilityPipeline _durabilityPipeline;
        private readonly ManifestRollPublisher _manifestRollPublisher;

        internal JournalEventLoopBridge(JournalCoordinator coordinator, JournalCoordinatorDurabilityPipeline durabilityPipeline, ManifestRollPublisher manifestRollPublisher)
        {
            _coordinator = coordinator;
            _durabilityPipeline = durabilityPipeline;
            _manifestRollPublisher = manifestRollPublisher;
        }

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint() => _durabilityPipeline.CompleteDurabilityCheckpointOnJournalThread();

        void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _coordinator.QueuedAppendsCounter.Value);

        void IJournalEventLoopHost.FailPipeline(Exception reason) => _durabilityPipeline.FailJournalPipeline(reason);

        void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex) => _manifestRollPublisher.PublishRoll(
            targetSegmentIndex,
            Volatile.Read(ref _coordinator.NextSequenceField),
            () => _durabilityPipeline.OnManifestRollSucceeded());

        int IJournalEventLoopHost.ReadQueuedAppends() => Volatile.Read(ref _coordinator.QueuedAppendsCounter.Value);

        void IJournalEventLoopHost.SetNextSequence(ulong value) => Volatile.Write(ref _coordinator.NextSequenceField, value);

        void IJournalEventLoopHost.ThrowIfJournalThreadFailed() => _durabilityPipeline.ThrowIfJournalThreadFailed();
    }
}
