using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer pipelined journal coordinator with binary frames (see docs/journal-binary-format.md).</summary>
internal sealed class JournalCoordinator : IJournalCoordinator, IJournalCoordinatorShutdown, IJournalCoordinatorAppendState, IJournalCoordinatorState,
    IJournalCoordinatorSnapshotState
{
    private const int RingCapacity = 4096;

    private static readonly TimeSpan DefaultGraceJoinFloor = TimeSpan.FromSeconds(5);

    private static readonly TimeSpan DefaultShutdownBudget = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan DefaultStageFloor = TimeSpan.FromSeconds(1);

    private static readonly ParameterizedThreadStart RunEventLoopCallback = static state =>
    {
        if (state is JournalEventLoop eventLoop)
            eventLoop.Run();
    };

    private readonly VolatileDouble _appendLatency = new();

    private readonly JournalCoordinatorAppendPipeline _appendPipeline;
    private readonly VolatileField<Exception> _flushLoopFailure = new();
    private readonly JournalSlowOperationReporter _slowOperations;
    private readonly JournalProducerGate _producerGate = new();

    private readonly JournalStopper _stopper;
    private long _bytes;
    private int _disposed;
    private ulong _nextSequence;
    private long _ops;

    internal JournalCoordinator(PersistenceOptions opt, State manifest, Ledger manifestStore, AsyncManualResetEvent startupGate, ILoggerFactory loggerFactory, TimeProvider? timeProvider = null)
        : this(opt, manifest, manifestStore, startupGate, JournalSegmentWriterFactory.Create(), loggerFactory, timeProvider)
    {
    }

    internal JournalCoordinator(
        PersistenceOptions opt,
        State manifest,
        Ledger manifestStore,
        AsyncManualResetEvent startupGate,
        IJournalSegmentWriter segmentWriter,
        ILoggerFactory loggerFactory,
        TimeProvider? timeProvider = null)
    {
        ArgumentNullException.ThrowIfNull(segmentWriter);
        ArgumentNullException.ThrowIfNull(loggerFactory);
        var log = loggerFactory.CreateLogger<JournalCoordinator>();
        var clock = timeProvider ?? TimeProvider.System;
        _slowOperations = new JournalSlowOperationReporter(log, clock);
        GraceJoinFloor = DefaultGraceJoinFloor;
        ShutdownBudget = DefaultShutdownBudget;
        StageFloor = DefaultStageFloor;
        Options = opt;
        Ledger = manifestStore;
        StartupGate = startupGate;
        StallProbe = new JournalStallProbe(log, clock);
        var probedWriter = new ProbedJournalSegmentWriter(segmentWriter, StallProbe);
        _stopper = new JournalStopper(this, probedWriter, log);
        _appendPipeline = new JournalCoordinatorAppendPipeline(this, _producerGate, clock);
        DurabilityPipeline = new JournalDurabilityCoordinator(this, this, loggerFactory.CreateLogger<JournalDurabilityCoordinator>(), _producerGate);
        var bridge = new JournalEventLoopBridge(this, DurabilityPipeline);
        var (segmentCount, totalBytes) = JournalReader.GetOnDiskSegmentStats(Options.DataDir);
        var currentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;

        // Taken after the factory's startup tail repair, which is the last startup step that changes the current segment.
        var activeSegment = JournalSegmentProbe.Probe(Options.DataDir, currentSegmentIndex);
        var eventLoopStartup = new JournalEventLoopStartup(currentSegmentIndex, totalBytes, segmentCount, activeSegment);
        EventLoop = new JournalEventLoop(bridge, Ring, probedWriter, Options, eventLoopStartup, loggerFactory.CreateLogger<JournalEventLoop>(), BackgroundCancellation.Token);
        GroupCommit = Options.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(EventLoop.FlushGroupCommitOnJournalThread, Ring.NotifyWorkAvailable, Options, onWaitCanceled: StallProbe.ReportWaitCanceled) : null;
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

    public JournalStallProbe StallProbe { get; }

    public JournalDurabilityGroupCommit? GroupCommit { get; }

    public long HighWaterBytes => EventLoop.Policy.HighWaterBytes;

    public QuiescenceGate InFlightApplyGate { get; } = new();

    public bool IsJournalGroupCommitEnabled => Options.IsJournalGroupCommitEnabled;

    public Thread JournalThread { get; }

    public Ledger Ledger { get; }

    public long MaxBytes => EventLoop.Policy.MaxTotalBytes;

    public AsyncLock MutationGate { get; } = new();

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public PersistenceOptions Options { get; }

    public PendingAppendRegistry PendingAppends { get; } = new();

    public MutableInt32 QueuedAppendsCounter { get; } = new();

    public double RecentAppendLatencyMs => _appendLatency.Read();

    public BoundedJournalRing Ring { get; } = new(RingCapacity);

    public AsyncManualResetEvent StartupGate { get; }

    public long UsedBytes => EventLoop.JournalTotalBytes;

    /// <summary>Gets the number of durability flushes (fsync calls) completed so far.</summary>
    internal long FlushCount => EventLoop.FlushCount;

    /// <summary>Gets the last join wait granted to the journal thread after the shutdown budget; 5 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The floor is not positive.</exception>
    internal TimeSpan GraceJoinFloor
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The grace join floor must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets the budget shared by the shutdown stages (quiescence, marker, and join); 30 seconds unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The budget is not positive.</exception>
    internal TimeSpan ShutdownBudget
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The shutdown budget must be greater than zero.");

            field = value;
        }
    }

    /// <summary>Gets the least wait each stop stage (quiescence, marker, first join) is granted once the shutdown budget is spent; 1 second unless set.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The floor is not positive.</exception>
    internal TimeSpan StageFloor
    {
        get;
        init
        {
            value.ThrowIfNegativeOrZero(nameof(value), "The stage floor must be greater than zero.");

            field = value;
        }
    }

    ulong IJournalCoordinatorAppendState.AllocateSequence(in AsyncLockOwnership ownership)
    {
        // A snapshot cut reads its watermark under the gate and relies on every covered frame being on the ring ahead of its checkpoint,
        // so a sequence is allocated (and its frame enqueued) only by a caller holding the gate. The ownership names the gate acquisition
        // it came from: this refuses, before anything is allocated or enqueued, a caller appending while the gate is free, with the
        // ownership of a barrier that already ended, or while another flow holds the gate.
        ownership.ThrowIfNotHeld(MutationGate, "journal appends must hold the mutation gate (ExecuteUnderSnapshotBarrierAsync).");

        while (true)
        {
            var current = Volatile.Read(ref _nextSequence);
            var next = current + 1UL;
            if (Interlocked.CompareExchange(ref _nextSequence, next, current) == current)
                return next;
        }
    }

    public ValueTask AppendIdempotencyOutcomeAsync(string operationId, string fingerprint, byte[] responseBytes, Action? appended, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(operationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(fingerprint);
        ArgumentNullException.ThrowIfNull(responseBytes);

        return ExecuteUnderSnapshotBarrierAsync(
            (Journal: this, Pipeline: _appendPipeline, OperationId: operationId, Fingerprint: fingerprint, ResponseBytes: responseBytes, Appended: appended),
            static async (state, ownership, ct) =>
            {
                // Entered after the mutation gate is held, mirroring DurableMutationExecutor: lets snapshot-cut
                // quiesce idempotency outcomes alongside cache mutations without risking a gate deadlock.
                state.Journal.InFlightApplyGate.Enter();
                try
                {
                    var record = state.Pipeline.AllocateIdempotencyRecord(in ownership, state.OperationId, state.Fingerprint, state.ResponseBytes);
                    await state.Pipeline.AppendRecordCoreAsync(record, ct).ConfigureAwait(false);
                    state.Appended?.Invoke();
                }
                finally
                {
                    state.Journal.InFlightApplyGate.Exit();
                }
            },
            cancellationToken);
    }

    public ValueTask AppendPutAsync(AsyncLockOwnership ownership, CacheKey key, ReadOnlyMemory<byte> entryBytes, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes.Span);
        return _appendPipeline.AppendRecordCoreAsync(_appendPipeline.AllocateRecord(in ownership, key, JournalOperationKind.Put, entryBytes), cancellationToken);
    }

    public ValueTask AppendRemoveAsync(AsyncLockOwnership ownership, CacheKey key, CancellationToken cancellationToken) => _appendPipeline.AppendRecordCoreAsync(
        _appendPipeline.AllocateRecord(in ownership, key, JournalOperationKind.Remove),
        cancellationToken);

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        DurabilityPipeline.ThrowIfJournalThreadFailed();
        _producerGate.ThrowIfShutdownInitiated();
        return GroupCommit?.AwaitCommitAsync(cancellationToken) ?? DurabilityPipeline.EnqueueFlushAsync(cancellationToken);
    }

    public ValueTask DisposeAsync() => Interlocked.Exchange(ref _disposed, 1) == 1 ? ValueTask.CompletedTask : _stopper.StopOnDisposeAsync();

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        DurabilityPipeline.ThrowIfJournalThreadFailed();
        await StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        AsyncLockHolder mutationGuard;
        try
        {
            mutationGuard = await MutationGate.LockAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            StallProbe.ReportWaitCanceled("mutation gate (maintenance)");
            throw;
        }

        var acquiredTimestamp = StallProbe.GateAcquired(nameof(ExecuteMaintenanceExclusiveAsync));
        try
        {
            await DurabilityPipeline.EnqueueMaintenanceAsync(action, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            StallProbe.GateReleased();
            mutationGuard.Dispose();
            _slowOperations.ReportMutationGateHold(acquiredTimestamp, nameof(ExecuteMaintenanceExclusiveAsync));
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
        var (seqAtFlush, barrierState, checkpoint) = await CaptureSnapshotCutAsync(state, captureUnderBarrier, cancellationToken).ConfigureAwait(false);

        // A faulted or canceled checkpoint aborts the cut: nothing is built or published over frames that may never become durable.
        await DurabilityPipeline.AwaitFlushAsync(checkpoint, cancellationToken).ConfigureAwait(false);
        return await buildOutsideBarrier(state, seqAtFlush, barrierState, cancellationToken).ConfigureAwait(false);
    }

    public ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(
        Func<AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
        CancellationToken cancellationToken) => ExecuteUnderSnapshotBarrierAsync(action, static (handler, ownership, ct) => handler(ownership, ct), cancellationToken);

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TState, TResult>(
        TState state,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask<TResult>> action,
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            StallProbe.ReportWaitCanceled("journal barrier");
            throw;
        }

        var acquiredTimestamp = StallProbe.GateAcquired(nameof(ExecuteUnderSnapshotBarrierAsync));
        try
        {
            return await action(state, gateGuard.Ownership, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            StallProbe.GateReleased();
            gateGuard.Dispose();
            _slowOperations.ReportMutationGateHold(acquiredTimestamp, nameof(ExecuteUnderSnapshotBarrierAsync));
        }
    }

    public async ValueTask ExecuteUnderSnapshotBarrierAsync<TState>(
        TState state,
        Func<TState, AsyncLockOwnership, CancellationToken, ValueTask> action,
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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            StallProbe.ReportWaitCanceled("journal barrier");
            throw;
        }

        var acquiredTimestamp = StallProbe.GateAcquired(nameof(ExecuteUnderSnapshotBarrierAsync));
        try
        {
            await action(state, gateGuard.Ownership, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            StallProbe.GateReleased();
            gateGuard.Dispose();
            _slowOperations.ReportMutationGateHold(acquiredTimestamp, nameof(ExecuteUnderSnapshotBarrierAsync));
        }
    }

    public void FailJournalPipeline(Exception reason) => DurabilityPipeline.FailJournalPipeline(reason);

    public Exception? GetJournalThreadFailure() => _flushLoopFailure.Read();

    /// <inheritdoc />
    public ValueTask StopAsync() => _stopper.StopAsync(ShutdownBudget);

    void IJournalCoordinatorAppendState.EnsureAppendAdmission(int frameLength)
    {
        // Conservative admission: a frame admitted here is never rejected for capacity by the journal thread. The caller holds
        // the mutation gate (AllocateSequence refused it otherwise), and the check runs synchronously up to this append's own Track, so no
        // other producer is admitted in between; meanwhile, the journal thread only moves appends out of the pending set.
        // - Read order: pending counters, then the journal thread's flags, then its counters. The journal thread accounts a frame's bytes (and
        //   the header and segment of an open or roll it triggers) and clears the flags before it untracks the frame, so a frame missing from
        //   the pending read is already reflected in everything read after it; and it counts a created segment before it clears the flag.
        // - Each pending frame, and this one, adds at most one segment header to the total (a roll, or the first open of a missing or empty
        //   segment; a frame that fits an empty segment never rolls right after such an open), so FileHeaderSize * (pendingCount + 1) bounds
        //   the headers added before this frame is checked.
        // - The journal thread rolls for this frame only if active + staged + frameLength exceeds the segment size. Either no roll happened
        //   since the read, so active + staged <= active + pendingBytes, or a pending frame overflowed the segment, so active + pendingBytes
        //   + frameLength overflows it too: either way the roll is predicted and the segment count checked. This holds because the active
        //   counter equals the current segment's on-disk length (as the next open sees it) whenever the segment is not open: it is seeded at
        //   startup and at maintenance end.
        // - The segments added before this frame's roll is checked are the open of a missing current segment (OpenCreatesSegment) and the
        //   rolls of the backlog, which JournalSegmentPolicy.BoundNewSegments bounds by the backlog's bytes, not only by its count; a roll
        //   into a pre-created target the journal already counted (RollTargetCounted) adds none.
        // - Apart from that forward accounting, the counters change (in either direction) only in the maintenance end resync. Maintenance
        //   holds the gate from before Begin until the End ack completes, even when its caller cancels, and the ring is empty by then, so no
        //   admission runs against a layout in flux. After a pipeline failure, appends are refused before they reach admission.
        var pendingBytes = PendingAppends.PendingBytes;
        var pendingCount = PendingAppends.PendingCount;
        var openCreatesSegment = EventLoop.OpenCreatesSegment;
        var rollTargetCounted = EventLoop.RollTargetCounted;
        var totalBytes = EventLoop.JournalTotalBytes;
        var activeSegmentBytes = EventLoop.ActiveSegmentWrittenBytes;
        var segmentCount = EventLoop.JournalSegmentCount;
        var snapshot = new JournalAdmissionSnapshot(pendingBytes, pendingCount, openCreatesSegment, rollTargetCounted, totalBytes, activeSegmentBytes, segmentCount);
        EventLoop.Policy.EnsureAdmissionOrThrow(in snapshot, frameLength);
    }

    void IJournalCoordinatorAppendState.RecordAppendMetrics(int frameLength, long startedMs)
    {
        var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
        var currentLatency = _appendLatency.Read();
        _appendLatency.Write(currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));
        _ = Interlocked.Add(ref _bytes, frameLength);
        _ = Interlocked.Increment(ref _ops);
        OnAppended?.Invoke(this, EventArgs.Empty);
    }

    public bool TrySetJournalThreadFailure(Exception reason) => _flushLoopFailure.TryWriteIfNull(reason);

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => StartupGate.WaitAsync(cancellationToken);

    /// <summary>Stops the journal within <paramref name="budget" />; see <see cref="IJournalCoordinatorShutdown.StopAsync" />.</summary>
    /// <param name="budget">Shared budget of the stop stages; each stage still gets its own floor once the budget is spent.</param>
    /// <returns>A task that completes when the journal is stopped.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="budget" /> is not positive.</exception>
    /// <exception cref="TimeoutException">A stop stage did not finish in time; the journal thread and its resources stay open.</exception>
    internal ValueTask StopAsync(TimeSpan budget) => _stopper.StopAsync(budget);

    private async ValueTask<(ulong Sequence, TBarrier BarrierState, TaskCompletionSource Checkpoint)> CaptureSnapshotCutAsync<TState, TBarrier>(
        TState state,
        Func<TState, ulong, CancellationToken, ValueTask<TBarrier>> captureUnderBarrier,
        CancellationToken cancellationToken)
    {
        var holder = await DurabilityPipeline.WaitForSnapshotCutAdmissionAsync(cancellationToken).ConfigureAwait(false);
        var acquiredTimestamp = StallProbe.GateAcquired(nameof(ExecuteSnapshotCutAsync));
        try
        {
            // Sequences are allocated and frames enqueued only under the gate, so a checkpoint published here
            // sits on the FIFO ring behind every frame the captured sequence covers; its ack is awaited after release.
            var checkpoint = await DurabilityPipeline.PublishFlushAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var sequence = NextSequence > 0 ? NextSequence - 1UL : 0UL;
                var barrierState = await captureUnderBarrier(state, sequence, cancellationToken).ConfigureAwait(false);
                return (sequence, barrierState, checkpoint);
            }
            catch
            {
                DurabilityPipeline.DetachDurabilityAck(checkpoint);
                throw;
            }
        }
        finally
        {
            StallProbe.GateReleased();
            holder.Dispose();
            _slowOperations.ReportMutationGateHold(acquiredTimestamp, nameof(ExecuteSnapshotCutAsync));
        }
    }

    /// <summary>Append encoding and ring enqueue for a journal coordinator.</summary>
    [Immutable]
    private sealed class JournalCoordinatorAppendPipeline
    {
        private readonly IJournalCoordinatorAppendState _owner;
        private readonly JournalProducerGate _producerGate;
        private readonly TimeProvider _timeProvider;

        internal JournalCoordinatorAppendPipeline(IJournalCoordinatorAppendState owner, JournalProducerGate producerGate, TimeProvider timeProvider)
        {
            _owner = owner;
            _producerGate = producerGate;
            _timeProvider = timeProvider;
        }

        internal JournalRecord AllocateIdempotencyRecord(in AsyncLockOwnership ownership, string operationId, string fingerprint, byte[] responseBytes)
        {
            // Allocated before renting: a caller refused for not holding the gate leaves no record out of the pool.
            var sequence = _owner.AllocateSequence(in ownership);
            var record = JournalRecord.RentForAppend();
            record.Sequence = sequence;
            record.UnixMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            record.Operation = JournalOperationKind.IdempotencyOutcome;
            record.Key = new CacheKey(string.Empty, string.Empty);
            record.IdempotencyOperationId = operationId;
            record.IdempotencyFingerprint = fingerprint;
            record.IdempotencyResponseBytes = responseBytes;
            return record;
        }

        internal JournalRecord AllocateRecord(in AsyncLockOwnership ownership, CacheKey key, JournalOperationKind operation, ReadOnlyMemory<byte> putEntryBytes = default)
        {
            // Allocated before renting: a caller refused for not holding the gate leaves no record out of the pool.
            var sequence = _owner.AllocateSequence(in ownership);
            var record = JournalRecord.RentForAppend();
            record.Sequence = sequence;
            record.UnixMs = _timeProvider.GetUtcNow().ToUnixTimeMilliseconds();
            record.Operation = operation;
            record.Key = key;
            record.PutEntryBytes = putEntryBytes;
            return record;
        }

        internal async ValueTask AppendRecordCoreAsync(JournalRecord record, CancellationToken cancellationToken)
        {
            var idempotencyStamped = StampIdempotencyOperationId(record);
            var cacheMutation = record.Operation is JournalOperationKind.Put or JournalOperationKind.Remove;
            _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
            await _owner.StartupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                var encode = BinaryJournalCodec.PrepareEncode(record);
                var frameLen = JournalFraming.FrameTotalLength(encode.BodyLength);

                // Refused before the buffer is rented, the frame is tracked, or its idempotency stamp is reported: nothing reaches the ring.
                _owner.EnsureAppendAdmission(frameLen);
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
                await EnqueueAppendAsync(frameBytes, frameLen, idempotencyStamped, cacheMutation, cancellationToken).ConfigureAwait(false);
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
                JournalOperationKind.Put or JournalOperationKind.Remove => RpcMutationIdempotencyExecutionAmbient.ActiveOperationIdValue,
                JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.MaintenanceExclusive or JournalOperationKind.SnapshotCut
                    or JournalOperationKind.UnderSnapshotBarrier or JournalOperationKind.IdempotencyOutcome
                    or JournalOperationKind.IdempotencyStarted => record.MutationOperationId,
                _ => record.MutationOperationId,
            };

            record.MutationOperationId = stampedOperationId;
            record.MutationFingerprint = stampedOperationId != null ? RpcMutationIdempotencyExecutionAmbient.ActiveFingerprintValue : null;
            return stampedOperationId != null;
        }

        /// <summary>Waits for the journal thread's write ack of a frame already on the ring.</summary>
        /// <param name="appendAck">Write ack of the enqueued frame.</param>
        /// <returns>An asynchronous operation.</returns>
        /// <exception cref="JournalPostEnqueueFaultException">Shutdown or the failure latch faulted the ack; the frame may still become durable.</exception>
        /// <exception cref="JournalCapacityExceededException">The journal thread rejected the frame before writing it.</exception>
        private static async ValueTask AwaitWriteAckAfterEnqueueAsync(TaskCompletionSource appendAck)
        {
            try
            {
                await appendAck.Task.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not JournalCapacityExceededException)
            {
                // Past the ring enqueue the caller cannot treat the fault as a failed append: mark it so it is reported as commit-unknown.
                throw new JournalPostEnqueueFaultException(JournalPostEnqueueFaultException.WriteAckFaultedMessage, ex);
            }
        }

        private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, bool idempotencyStamped, bool cacheMutation, CancellationToken cancellationToken)
        {
            var appendAck = _owner.Options.IsJournalGroupCommitEnabled ? new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) : null;
            var item = JournalWorkItem.Append(frameBytes, frameLength, appendAck);
            await EnqueueTrackedAppendAsync(item, frameBytes, frameLength, appendAck, cancellationToken).ConfigureAwait(false);

            // The frame is on the ring and may become durable even if the write ack below faults (shutdown or the failure latch after the
            // write reached the file), so the stamp is reported before that wait: the idempotency intent must survive such a fault.
            if (idempotencyStamped)
                RpcMutationIdempotencyExecutionAmbient.NotifyMutationStamped();
            else if (cacheMutation)
                RpcMutationIdempotencyExecutionAmbient.NotifyMutationApplied();

            // The durability wait stays outside the gate: the gate covers only the publishing, so a
            // slow journal thread never blocks shutdown drain on fsync latency.
            if (appendAck != null)
                await AwaitWriteAckAfterEnqueueAsync(appendAck).ConfigureAwait(false);
        }

        private async ValueTask EnqueueTrackedAppendAsync(JournalWorkItem item, byte[] frameBytes, int frameLength, TaskCompletionSource? trackAck, CancellationToken cancellationToken)
        {
            // Increment first (as before): the slot is owned from admission, so a drain racing
            // below always balances. The Track-failure path compensates symmetrically.
            _ = Interlocked.Increment(ref _owner.QueuedAppendsCounter.Value);
            try
            {
                // Track before the ring enqueue (and its semaphore wait) so the fail-fast latch,
                // not the semaphore, bounds producers after a failure drain.
                _owner.PendingAppends.Track(item, frameBytes, frameLength, trackAck);
            }
            catch
            {
                // Fail-fast after a drain: the frame never entered the ring, so compensate the
                // slot and return the rented buffer here. Surface the pipeline failure, not the latch.
                _ = Interlocked.Decrement(ref _owner.QueuedAppendsCounter.Value);
                ArrayPool<byte>.Shared.ReturnCleared(frameBytes);
                _owner.DurabilityPipeline.ThrowIfJournalThreadFailed();
                throw;
            }

            _producerGate.Enter();
            try
            {
                // Shutdown rejection lands in the same cleanup below: the frame never entered
                // the ring, so the buffer and the queued-append slot are released here.
                _producerGate.ThrowIfShutdownInitiated();
                await _owner.Ring.EnqueueAsync(item, cancellationToken, _owner.DurabilityPipeline.ThrowIfJournalThreadFailedCheck).ConfigureAwait(false);
            }
            catch when (_owner.PendingAppends.Untrack(item, out _))
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

        PendingAppendRegistry IJournalEventLoopHost.PendingAppends => _coordinator.PendingAppends;

        void IJournalEventLoopHost.CompleteDurabilityCheckpoint(JournalWorkItem item) => _durabilityPipeline.CompleteCheckpointOnJournalThread(item);

        void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _coordinator.QueuedAppendsCounter.Value);

        void IJournalEventLoopHost.FailPipeline(Exception reason) => _durabilityPipeline.FailJournalPipeline(reason);

        void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex)
        {
            var sequence = Volatile.Read(ref _coordinator._nextSequence);
            _coordinator.Ledger.EnqueueRoll(targetSegmentIndex, sequence, _durabilityPipeline.OnManifestRollSucceeded, _durabilityPipeline.OnManifestRollFailed);
        }

        void IJournalEventLoopHost.SetNextSequence(ulong value) => Volatile.Write(ref _coordinator._nextSequence, value);

        void IJournalEventLoopHost.ThrowIfJournalThreadFailed() => _durabilityPipeline.ThrowIfJournalThreadFailed();
    }

    /// <summary>Records every segment I/O call in a <see cref="JournalStallProbe" /> so a stalled journal thread is visible while it is blocked.</summary>
    [Immutable]
    private sealed class ProbedJournalSegmentWriter : IJournalSegmentWriter
    {
        private readonly IJournalSegmentWriter _inner;
        private readonly JournalStallProbe _probe;

        internal ProbedJournalSegmentWriter(IJournalSegmentWriter inner, JournalStallProbe probe)
        {
            ArgumentNullException.ThrowIfNull(inner);
            ArgumentNullException.ThrowIfNull(probe);
            _inner = inner;
            _probe = probe;
        }

        public long Length => _inner.Length;

        public void Dispose() => _inner.Dispose();

        public void FlushToDisk()
        {
            _probe.IoStarted(nameof(FlushToDisk));
            try
            {
                _inner.FlushToDisk();
            }
            finally
            {
                _probe.IoFinished();
            }
        }

        public void OpenSegment(string path, bool append)
        {
            _probe.IoStarted(nameof(OpenSegment));
            try
            {
                _inner.OpenSegment(path, append);
            }
            finally
            {
                _probe.IoFinished();
            }
        }

        public void Truncate(long length)
        {
            _probe.IoStarted(nameof(Truncate));
            try
            {
                _inner.Truncate(length);
            }
            finally
            {
                _probe.IoFinished();
            }
        }

        public void Write(ReadOnlySpan<byte> buffer, long fileOffset)
        {
            _probe.IoStarted(nameof(Write));
            try
            {
                _inner.Write(buffer, fileOffset);
            }
            finally
            {
                _probe.IoFinished();
            }
        }
    }
}
