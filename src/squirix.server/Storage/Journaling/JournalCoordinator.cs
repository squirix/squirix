using System;
using System.Buffers;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
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

    private static readonly TimeSpan ShutdownBudget = TimeSpan.FromSeconds(30);

    private readonly VolatileDouble _appendLatency = new();

    private readonly JournalCoordinatorAppendPipeline _appendPipeline;
    private readonly VolatileField<Exception> _flushLoopFailure = new();
    private readonly JournalProducerGate _producerGate = new();

    private readonly IJournalSegmentWriter _segmentWriter;
    private long _bytes;
    private int _disposed;
    private ulong _nextSequence;
    private long _ops;

    internal JournalCoordinator(PersistenceOptions opt, State manifest, Ledger manifestStore, AsyncManualResetEvent startupGate)
    {
        Options = opt;
        Ledger = manifestStore;
        StartupGate = startupGate;
        _segmentWriter = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        _appendPipeline = new JournalCoordinatorAppendPipeline(this, _producerGate);
        DurabilityPipeline = new JournalDurabilityCoordinator(this, this, LogManager.GetLogger<JournalDurabilityCoordinator>(), _producerGate);
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

    public CancellationTokenSource BackgroundCancellation { get; } = new();

    public int CurrentSegmentIndex => EventLoop.CurrentSegmentIndex;

    public DurabilityAckRegistry DurabilityAcks { get; } = new();

    public MutableInt32 DurabilityFlushScheduledFlag { get; } = new();

    public JournalDurabilityCoordinator DurabilityPipeline { get; }

    public JournalEventLoop EventLoop { get; }

    public JournalDurabilityGroupCommit? GroupCommit { get; }

    public bool HasFlushLoopFailure => _flushLoopFailure.Read() != null;

    public long HighWaterBytes => EventLoop.Policy.HighWaterBytes;

    public QuiescenceGate InFlightApplyGate { get; } = new();

    public bool IsJournalGroupCommitEnabled => Options.IsJournalGroupCommitEnabled;

    public Thread JournalThread { get; }

    public Ledger Ledger { get; }

    public long MaxBytes => EventLoop.Policy.MaxTotalBytes;

    public AsyncLock MutationGate { get; } = new();

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public PersistenceOptions Options { get; }

    public MutableInt32 QueuedAppendsCounter { get; } = new();

    public double RecentAppendLatencyMs => _appendLatency.Read();

    public BoundedJournalRing Ring { get; } = new(RingCapacity);

    public AsyncManualResetEvent StartupGate { get; }

    public long UsedBytes => EventLoop.JournalTotalBytes;

    internal long ActiveSegmentWrittenBytes => EventLoop.ActiveSegmentWrittenBytes;

    private ILogger JournalLog => field ??= LogManager.GetLogger<JournalCoordinator>();

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

    public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        return ExecuteUnderSnapshotBarrierAsync(
            (Journal: this, Pipeline: _appendPipeline, OperationId: operationId, Fingerprint: fingerprint, ResponseBytes: responseBytes),
            static async (state, ct) =>
            {
                // Entered after the mutation gate is held, mirroring DurableMutationExecutor: lets snapshot-cut
                // quiesce idempotency outcomes alongside cache mutations without risking a gate deadlock.
                state.Journal.InFlightApplyGate.Enter();
                try
                {
                    var record = state.Pipeline.AllocateIdempotencyRecord(state.OperationId, state.Fingerprint, state.ResponseBytes);
                    await state.Pipeline.AppendRecordCoreAsync(record, ct).ConfigureAwait(false);
                }
                finally
                {
                    state.Journal.InFlightApplyGate.Exit();
                }
            },
            cancellationToken);
    }

    public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes.Span);
        return Options.IsJournalGroupCommitEnabled ? _appendPipeline.AppendPutAndAwaitDurabilityAsync(key, entryBytes, cancellationToken)
            : _appendPipeline.AppendRecordWithDurabilityCoreAsync(_appendPipeline.AllocateRecord(key, JournalOperationKind.Put, entryBytes), cancellationToken);
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
        _producerGate.ThrowIfShutdownInitiated();
        return GroupCommit?.AwaitCommitAsync(cancellationToken) ?? DurabilityPipeline.FlushAsync(cancellationToken);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var failures = new List<Exception>();

        // All shutdown stages share one budget (the host default): quiescence, marker, join, and
        // grace join must fit it cumulatively instead of stacking independent fixed waits.
        var shutdownDeadline = Environment.TickCount64 + Convert.ToInt64(ShutdownBudget.TotalMilliseconds);

        // Quiesce producers BEFORE the shutdown marker enters the ring: the gate guarantees every
        // admitted enqueue is published ahead of the marker (ring FIFO), and work arriving after
        // shutdown is rejected explicitly instead of being silently dropped or hung.
        await QuiesceProducersAsync(failures, RemainingBeforeShutdown(shutdownDeadline)).ConfigureAwait(false);

        // The shutdown marker must enter the ring BEFORE background cancellation is requested: the
        // journal thread dequeues FIFO, so every item enqueued before it is drained and written, and
        // only then does the thread observe Shutdown and exit. Cancelling first would let the thread
        // exit via OperationCanceledException while frames were still queued, silently dropping them.
        await DurabilityPipeline.EnqueueShutdownMarkerAsync(failures, RemainingBeforeShutdown(shutdownDeadline)).ConfigureAwait(false);
        await DurabilityPipeline.AwaitJournalThreadDuringDisposeAsync(failures, RemainingBeforeShutdown(shutdownDeadline)).ConfigureAwait(false);

        try
        {
            await BackgroundCancellation.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown can dispose the CTS before cancellation is observed.
            LogManager.JournalBackgroundCancellationDisposedOnDispose(JournalLog);
        }

        GroupCommit?.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator)));
        DurabilityPipeline.FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));

        if (JournalThread.IsAlive && !await DurabilityPipeline.TryJoinJournalThreadAsync(RemainingBeforeShutdown(shutdownDeadline)).ConfigureAwait(false))
        {
            // The join timed out: tearing down the writer, ring, or gates under a live journal
            // thread corrupts slot accounting and races in-flight writes. Leak them instead and
            // surface the timeout loudly alongside any earlier stage failures.
            LogManager.JournalThreadLeakedOnShutdownTimeout(JournalLog);
            failures.Add(new TimeoutException("journal I/O thread is still alive after shutdown; writer, ring, and gates are leaked."));
            JournalDurabilityCoordinator.ThrowDisposeFailures(failures);
            return;
        }

        _segmentWriter.Dispose();
        Ring.Dispose();
        BackgroundCancellation.Dispose();
        MutationGate.Dispose();
        JournalDurabilityCoordinator.ThrowDisposeFailures(failures);

        static TimeSpan RemainingBeforeShutdown(long shutdownDeadline)
        {
            var remainingMs = shutdownDeadline - Environment.TickCount64;
            return remainingMs <= 0 ? TimeSpan.Zero : TimeSpan.FromMilliseconds(remainingMs);
        }
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        DurabilityPipeline.ThrowIfJournalThreadFailed();
        await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        using var mutationGuard = await MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
        await DurabilityPipeline.EnqueueMaintenanceAsync(action, cancellationToken).ConfigureAwait(false);
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
        var (seqAtFlush, barrierState) = await CaptureSnapshotCutAsync(state, captureUnderBarrier, cancellationToken).ConfigureAwait(false);
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

        AsyncLockHolder gateGuard;
        try
        {
            await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            gateGuard = await MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
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
            gateGuard.Dispose();
        }
    }

    public async ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(TState state, Func<TState, CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        DurabilityPipeline.ThrowIfJournalThreadFailed();

        AsyncLockHolder gateGuard;
        try
        {
            await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            gateGuard = await MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            throw new InvalidOperationException("journal coordinator is disposed.", ex);
        }

        try
        {
            await action(state, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            gateGuard.Dispose();
        }
    }

    public Exception? GetJournalThreadFailure() => _flushLoopFailure.Read();

    void IJournalCoordinatorAppendState.RecordAppendMetrics(int frameLength, long startedMs)
    {
        var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
        var currentLatency = _appendLatency.Read();
        _appendLatency.Write(currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));
        _ = Interlocked.Add(ref _bytes, frameLength);
        _ = Interlocked.Increment(ref _ops);
        NotifyAppended();
    }

    public void SetJournalThreadFailure(Exception? value) => _flushLoopFailure.Write(value);

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => StartupGate.WaitAsync(cancellationToken);

    private async ValueTask<(ulong Sequence, TBarrier BarrierState)> CaptureSnapshotCutAsync<TState, TBarrier>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        CancellationToken cancellationToken)
    {
        using var holder = await DurabilityPipeline.WaitForSnapshotCutAdmissionAsync(cancellationToken).ConfigureAwait(false);
        await DurabilityPipeline.FlushAsync(cancellationToken).ConfigureAwait(false);
        var sequence = NextSequence > 0 ? NextSequence - 1UL : 0UL;
        var barrierState = await captureUnderBarrier(state, sequence, cancellationToken).ConfigureAwait(false);
        return (sequence, barrierState);
    }

    private void NotifyAppended() => OnAppended?.Invoke(this, EventArgs.Empty);

    /// <summary>Quiesces producers so the shutdown marker cannot overtake an admitted enqueue.</summary>
    /// <param name="failures">Disposal failures to record a quiescence timeout into.</param>
    /// <param name="remaining">Time left in the shared shutdown budget.</param>
    /// <returns>A task that completes when producers quiesced, or throws loudly when they did not.</returns>
    private async ValueTask QuiesceProducersAsync(List<Exception> failures, TimeSpan remaining)
    {
        _producerGate.InitiateShutdown();
        if (await _producerGate.WaitAsync(remaining).ConfigureAwait(false))
            return;

        // Producers never quiesced: publishing the marker now could let it overtake an admitted
        // append. Fail reachable waiters explicitly and stop instead of proceeding into
        // marker/join/teardown with a broken ordering guarantee.
        LogManager.JournalProducerQuiescenceTimedOut(JournalLog);
        GroupCommit?.CancelPending(new ObjectDisposedException(nameof(JournalCoordinator)));
        DurabilityPipeline.FailPendingDurabilityAcks(new ObjectDisposedException(nameof(JournalCoordinator)));
        failures.Add(new TimeoutException("journal producers did not quiesce within the shutdown budget."));
        JournalDurabilityCoordinator.ThrowDisposeFailures(failures);
    }

    /// <summary>Append encoding and ring enqueue for a journal coordinator.</summary>
    [Immutable]
    private sealed class JournalCoordinatorAppendPipeline
    {
        private readonly IJournalCoordinatorAppendState _owner;
        private readonly JournalProducerGate _producerGate;

        internal JournalCoordinatorAppendPipeline(IJournalCoordinatorAppendState owner, JournalProducerGate producerGate)
        {
            _owner = owner;
            _producerGate = producerGate;
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

        internal async ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
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
            var idempotencyStamped = StampIdempotencyOperationId(record);
            _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
            await _owner.StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var encode = BinaryJournalCodec.PrepareEncode(record);
                var frameLen = JournalFraming.FrameTotalLength(encode.BodyLength);
                var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                try
                {
                    _ = BinaryJournalCodec.Encode(record, frameBytes.AsSpan(bodyOffset, encode.BodyLength), in encode);
                    JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), frameBytes.AsSpan(bodyOffset, encode.BodyLength));
                }
                catch
                {
                    ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                    throw;
                }

                var startedMs = Environment.TickCount64;
                await EnqueueAppendAsync(frameBytes, frameLen, cancellationToken).ConfigureAwait(false);
                if (idempotencyStamped)
                    RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
                _owner.RecordAppendMetrics(frameLen, startedMs);
            }
            finally
            {
                record.ReturnToAppendPool();
            }
        }

        internal async ValueTask AppendRecordWithDurabilityCoreAsync(JournalRecord record, CancellationToken cancellationToken)
        {
            var idempotencyStamped = StampIdempotencyOperationId(record);
            _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
            await _owner.StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var encode = BinaryJournalCodec.PrepareEncode(record);
                var frameLen = JournalFraming.FrameTotalLength(encode.BodyLength);
                var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                try
                {
                    _ = BinaryJournalCodec.Encode(record, frameBytes.AsSpan(bodyOffset, encode.BodyLength), in encode);
                    JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), frameBytes.AsSpan(bodyOffset, encode.BodyLength));
                }
                catch
                {
                    ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                    throw;
                }

                var startedMs = Environment.TickCount64;
                var ack = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                await EnqueueAppendWithDurabilityAsync(frameBytes, frameLen, ack, cancellationToken).ConfigureAwait(false);
                if (idempotencyStamped)
                    RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
                await ack.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);

                _owner.RecordAppendMetrics(frameLen, startedMs);
            }
            finally
            {
                record.ReturnToAppendPool();
            }
        }

        /// <summary>
        /// Stamps mutation frames appended inside an idempotent RPC scope with the active operation id. The durable
        /// frame becomes the write-ahead intent: recovery can reconstruct "started but outcome unknown" records from
        /// it and refuse to re-execute the mutation after a crash.
        /// </summary>
        /// <param name="record">The record about to be encoded and enqueued.</param>
        /// <returns>
        /// <see langword="true" /> when the record was stamped from the ambient scope. The caller reports it via
        /// <see cref="RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped" /> only after the frame is
        /// successfully enqueued: a failure before enqueue (encode, gate, or ring) leaves the idempotency
        /// reservation retryable instead of pinning it as outcome-unknown.
        /// </returns>
        private static bool StampIdempotencyOperationId(JournalRecord record)
        {
            if (record.MutationOperationId != null)
                return false;

            var stampedOperationId = record.Operation switch
            {
                JournalOperationKind.Put or JournalOperationKind.Remove or JournalOperationKind.RemoveExpiration or JournalOperationKind.TouchExpiration =>
                    RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue,
                _ => record.MutationOperationId,
            };

            record.MutationOperationId = stampedOperationId;
            return stampedOperationId != null;
        }

        private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            var appendAck = _owner.Options.IsJournalGroupCommitEnabled ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : null;
            var enqueued = false;
            _producerGate.Enter();
            try
            {
                // Inside the cleanup scope: rejecting here still returns the frame buffer and
                // releases the queued-append slot through the !enqueued path below.
                _producerGate.ThrowIfShutdownInitiated();
                var item = JournalWorkItem.Append(frameBytes, frameLength, appendAck);
                await _owner.Ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
                enqueued = true;
            }
            catch when (!enqueued)
            {
                ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                throw;
            }
            finally
            {
                _producerGate.Exit();
            }

            // The durability wait stays outside the gate: the gate covers only the publishing, so a
            // slow journal thread never blocks shutdown drain on fsync latency.
            if (appendAck != null)
                await appendAck.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
        }

        private async ValueTask EnqueueAppendWithDurabilityAsync(byte[] frameBytes, int frameLength, TaskCompletionSource ack, CancellationToken cancellationToken)
        {
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            var enqueued = false;
            _producerGate.Enter();
            try
            {
                // Inside the cleanup scope: rejecting here still returns the frame buffer and
                // releases the queued-append slot through the !enqueued path below.
                _producerGate.ThrowIfShutdownInitiated();
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
            finally
            {
                _producerGate.Exit();
            }
        }
    }

    /// <summary>
    /// Forwards <see cref="IJournalEventLoopHost" /> callbacks from <see cref="JournalEventLoop" />
    /// to <see cref="JournalCoordinator" /> without the coordinator implementing the interface directly.
    /// </summary>
    [Immutable]
    private sealed class JournalEventLoopBridge : IJournalEventLoopHost
    {
        private readonly JournalCoordinator _coordinator;
        private readonly JournalDurabilityCoordinator _durabilityPipeline;

        internal JournalEventLoopBridge(JournalCoordinator coordinator, JournalDurabilityCoordinator durabilityPipeline)
        {
            _coordinator = coordinator;
            _durabilityPipeline = durabilityPipeline;
        }

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _durabilityPipeline.CompleteCheckpointOnJournalThread(item);

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
}
