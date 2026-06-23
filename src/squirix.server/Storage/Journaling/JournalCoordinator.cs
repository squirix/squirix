using System;
using System.Buffers;
using System.Collections.Generic;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer pipelined journal coordinator with binary frames (see docs/journal-binary-format.md).</summary>
internal sealed class JournalCoordinator : IJournalCoordinator, IJournalEventLoopHost
{
    private const int RingCapacity = 4096;
    private readonly CancellationTokenSource _bgCts = new();
    private readonly Lock _durabilityWaitersLock = new();
    private readonly JournalEventLoop _eventLoop;
    private readonly JournalDurabilityGroupCommit? _groupCommit;
    private readonly Thread _journalThread;
    private readonly ManifestRollPublisher _manifestRollPublisher;
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly Lock _pendingMemoryApplyLock = new();

    private readonly BoundedJournalRing _ring = new(RingCapacity);
    private readonly IJournalSegmentWriter _segmentWriter;
    private readonly JournalStartupGate _startupGate;
    private double _avgAppendLatencyMs;
    private long _bytes;
    private int _disposed;
    private int _durabilityFlushScheduled;
    private List<JournalDurabilityWaiter>? _durabilityWaiters;
    private Exception? _journalThreadFailure;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;
    private int _queuedAppends;

    private JournalCoordinator(PersistenceOptions opt, ManifestState manifest, ManifestStore manifestStore, JournalStartupGate startupGate)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _startupGate = startupGate;
        _segmentWriter = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        var onDiskStats = JournalReader.GetOnDiskSegmentStats(_opt.DataDir);
        var currentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        _eventLoop = new JournalEventLoop(this, _ring, _segmentWriter, _opt, currentSegmentIndex, onDiskStats.TotalBytes, onDiskStats.SegmentCount, _bgCts.Token);
        _manifestRollPublisher = new ManifestRollPublisher(manifestStore, OnManifestRollFailed);
        _groupCommit = _opt.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(_eventLoop.FlushGroupCommitOnJournalThread, () => _ring.NotifyWorkAvailable(), _opt) : null;
        _eventLoop.AttachGroupCommit(_groupCommit);
        _ = DirectoryEx.CreateDirectory(_opt.DataDir);
        _nextSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _opt);
        _journalThread = new Thread(_eventLoop.Run) { IsBackground = true, Name = "squirix-journal-io" };
        _journalThread.Start();
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex => _eventLoop.CurrentSegmentIndex;

    public bool HasFlushLoopFailure => Volatile.Read(ref _journalThreadFailure) is not null;

    public bool IsJournalGroupCommitEnabled => _opt.IsJournalGroupCommitEnabled;

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public double RecentAppendLatencyMs => Volatile.Read(ref _avgAppendLatencyMs);

    internal long ActiveSegmentWrittenBytes => _eventLoop.ActiveSegmentWrittenBytes;

    internal bool IsDurabilityFlushPending => _eventLoop.IsDurabilityFlushPending;

    public static async Task<JournalCoordinator> CreateAsync(
        PersistenceOptions opt,
        ManifestState manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        await JournalRecoveryScan.PrepareActiveSegmentForSequenceScanAsync(manifest, opt, cancellationToken).ConfigureAwait(false);
        return new JournalCoordinator(opt, manifest, manifestStore, startupGate);
    }

    public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, byte[] entryBytes, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes);
        if (_opt.IsJournalGroupCommitEnabled)
        {
            return AppendPutAndAwaitDurabilityViaGroupCommitAsync(key, entryBytes, operationId, cancellationToken);
        }

        return AppendRecordWithDurabilityCoreAsync(AllocateRecord(key, JournalOperationKind.Put, entryBytes, operationId ?? string.Empty), cancellationToken);
    }

    public ValueTask AppendPutAsync(CacheKey key, byte[] entryBytes, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureEntryBytesWithinLimit(entryBytes);
        return AppendRecordCoreAsync(AllocateRecord(key, JournalOperationKind.Put, entryBytes, operationId ?? string.Empty), cancellationToken);
    }

    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => AppendRecordCoreAsync(
        AllocateRecord(key, JournalOperationKind.Remove),
        cancellationToken);

    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => AppendRecordCoreAsync(
        AllocateRecord(key, JournalOperationKind.RemoveExpiration),
        cancellationToken);

    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => AppendRecordCoreAsync(
        AllocateRecord(key, JournalOperationKind.TouchExpiration, touchExpirationUtc: expiresUtc),
        cancellationToken);

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        ThrowIfJournalThreadFailed();
        return _groupCommit?.AwaitCommitAsync(cancellationToken) ?? FlushAsync(cancellationToken);
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
            await _bgCts.CancelAsync().ConfigureAwait(false);
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown can dispose the CTS before cancellation is observed.
        }

        if (_groupCommit is not null)
            await _groupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(JournalCoordinator))).ConfigureAwait(false);

        FailPendingDurabilityWaiters(new ObjectDisposedException(nameof(JournalCoordinator)));

        await EnqueueShutdownAsync().ConfigureAwait(false);
        await AwaitJournalThreadDuringDisposeAsync(failures).ConfigureAwait(false);
        await _segmentWriter.DisposeAsync().ConfigureAwait(false);
        _manifestRollPublisher.Dispose();
        _ring.Dispose();
        _bgCts.Dispose();
        _mutationGate.Dispose();
        ThrowDisposeFailures(failures);
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        ThrowIfJournalThreadFailed();
        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await EnqueueMaintenanceAsync(action, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _mutationGate.Release();
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
        ThrowIfJournalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await WaitForSnapshotCutAdmissionAsync(cancellationToken).ConfigureAwait(false);
        ulong seqAtFlush;
        TBarrier barrierState;
        try
        {
            await FlushAsync(cancellationToken).ConfigureAwait(false);
            seqAtFlush = NextSequence > 0 ? NextSequence - 1UL : 0UL;
            barrierState = await captureUnderBarrier(state, seqAtFlush, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _mutationGate.Release();
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
        ThrowIfJournalThreadFailed();

        try
        {
            await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
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
            _ = _mutationGate.Release();
        }
    }

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _startupGate.WaitAsync(cancellationToken);

    void IJournalEventLoopHost.ThrowIfJournalThreadFailed() => ThrowIfJournalThreadFailed();

    void IJournalEventLoopHost.FailPipeline(Exception reason) => FailJournalPipeline(reason);

    void IJournalEventLoopHost.CompleteDurabilityCheckpoint() => CompleteDurabilityCheckpointOnJournalThread();

    void IJournalEventLoopHost.PublishRoll(int targetSegmentIndex) =>
        _manifestRollPublisher.PublishRoll(targetSegmentIndex, Volatile.Read(ref _nextSequence), OnManifestRollSucceeded);

    int IJournalEventLoopHost.ReadQueuedAppends() => Volatile.Read(ref _queuedAppends);

    void IJournalEventLoopHost.DecrementQueuedAppends() => _ = Interlocked.Decrement(ref _queuedAppends);

    void IJournalEventLoopHost.SetNextSequence(ulong value) => Volatile.Write(ref _nextSequence, value);

    private static void ThrowDisposeFailures(List<Exception> failures)
    {
        switch (failures.Count)
        {
            case 0:
                return;
            case 1:
                ExceptionDispatchInfo.Capture(failures[0]).Throw();
                break;
            default:
                throw new AggregateException("journal coordinator disposal failed.", failures);
        }
    }

    private JournalRecord AllocateRecord(
        CacheKey key,
        JournalOperationKind operation,
        byte[]? putEntryBytes = null,
        string? putOperationId = null,
        DateTime? touchExpirationUtc = null)
    {
        var record = JournalRecord.RentForAppend();
        record.Sequence = AllocateSequence();
        record.UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        record.Operation = operation;
        record.Key = key;
        record.PutEntryBytes = putEntryBytes;
        record.PutOperationId = putOperationId;
        record.TouchExpirationUtc = touchExpirationUtc;
        return record;
    }

    private ulong AllocateSequence()
    {
        while (true)
        {
            var current = Volatile.Read(ref _nextSequence);
            var next = current + 1UL;
            if (Interlocked.CompareExchange(ref _nextSequence, next, current) == current)
                return next;
        }
    }

    private async ValueTask AppendPutAndAwaitDurabilityViaGroupCommitAsync(CacheKey key, byte[] entryBytes, string? operationId, CancellationToken cancellationToken)
    {
        await AppendPutAsync(key, entryBytes, operationId, cancellationToken).ConfigureAwait(false);
        await AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AppendRecordCoreAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        ThrowIfJournalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var (bodyLen, keyUtf8) = BinaryJournalCodec.PrepareEncode(record);
            var frameLen = JournalFraming.FrameTotalLength(bodyLen);
            var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
            var body = frameBytes.AsSpan(JournalFraming.FrameHeaderSize, bodyLen);
            _ = BinaryJournalCodec.Encode(record, body, keyUtf8);
            JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), body);

            var startedMs = Environment.TickCount64;
            await EnqueueAppendAsync(frameBytes, frameLen, cancellationToken).ConfigureAwait(false);
            var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
            var currentLatency = Volatile.Read(ref _avgAppendLatencyMs);
            Volatile.Write(ref _avgAppendLatencyMs, currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));

            _ = Interlocked.Add(ref _bytes, frameLen);
            _ = Interlocked.Increment(ref _ops);
            OnAppended?.Invoke(this, EventArgs.Empty);
        }
        finally
        {
            record.ReturnToAppendPool();
        }
    }

    private async ValueTask AppendRecordWithDurabilityCoreAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        ThrowIfJournalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var (bodyLen, keyUtf8) = BinaryJournalCodec.PrepareEncode(record);
            var frameLen = JournalFraming.FrameTotalLength(bodyLen);
            var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
            var body = frameBytes.AsSpan(JournalFraming.FrameHeaderSize, bodyLen);
            _ = BinaryJournalCodec.Encode(record, body, keyUtf8);
            JournalFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), body);

            var startedMs = Environment.TickCount64;
            var waiter = JournalDurabilityWaiter.Rent();
            try
            {
                // Wait without cancellation: once the frame is enqueued the journal thread owns the
                // waiter and will complete it. Cancellation only aborts the pre-enqueue backpressure
                // wait inside EnqueueAppendWithDurabilityAsync, which leaves the waiter untouched.
                var waitTask = waiter.AwaitAsync(CancellationToken.None);
                await EnqueueAppendWithDurabilityAsync(frameBytes, frameLen, waiter, cancellationToken).ConfigureAwait(false);
                await waitTask.ConfigureAwait(false);
            }
            finally
            {
                waiter.ReturnToPool();
            }

            var elapsedMs = Math.Max(0, Environment.TickCount64 - startedMs);
            var currentLatency = Volatile.Read(ref _avgAppendLatencyMs);
            Volatile.Write(ref _avgAppendLatencyMs, currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));

            _ = Interlocked.Add(ref _bytes, frameLen);
            _ = Interlocked.Increment(ref _ops);
            OnAppended?.Invoke(this, EventArgs.Empty);
        }
        finally
        {
            record.ReturnToAppendPool();
        }
    }

    private async ValueTask AwaitJournalThreadDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
            if (!await Task.Factory.StartNew(
                    () => _journalThread.Join(TimeSpan.FromSeconds(30)),
                    _bgCts.Token,
                    TaskCreationOptions.LongRunning | TaskCreationOptions.DenyChildAttach,
                    TaskScheduler.Default).ConfigureAwait(false))
            {
                failures.Add(new TimeoutException("journal I/O thread did not exit within 30 seconds."));
            }
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // Dispose Canceled the join wait when teardown already completed.
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
    }

    private void CompleteDurabilityCheckpointOnJournalThread()
    {
        List<JournalDurabilityWaiter>? waiters;
        lock (_durabilityWaitersLock)
        {
            if (_durabilityWaiters is null || _durabilityWaiters.Count is 0)
            {
                _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
                return;
            }

            waiters = _durabilityWaiters;
            _durabilityWaiters = null;
        }

        _eventLoop.FsyncOnJournalThread();

        foreach (var waiter in waiters)
            _ = waiter.TrySetResult();

        _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
    }

    private void DetachDurabilityWaiter(JournalDurabilityWaiter waiter)
    {
        lock (_durabilityWaitersLock)
            _ = _durabilityWaiters?.Remove(waiter);
    }

    private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _queuedAppends);

        // The per-append Completion waiter (group-commit mode only) is deliberately retained rather
        // than removed (audit item A1): it holds the producer until its frame has been staged+written,
        // which is the ordering invariant that lets DrainDueGroupCommitBatchesDuringRoll complete
        // durability waiters safely while a segment roll is in flight.
        var appendCompleted = _opt.IsJournalGroupCommitEnabled ? JournalDurabilityWaiter.Rent() : null;

        // The completion waiter is awaited without cancellation: once the frame is published to the
        // ring it is owned by the journal thread, which alone decrements _queuedAppends, returns the
        // frame buffer, and completes this waiter. Cancellation must only abort the pre-enqueue
        // backpressure wait inside EnqueueAsync; after a successful enqueue the producer must wait for
        // the journal thread instead of decrementing the counter or recycling the waiter a second time.
        var appendWaitTask = appendCompleted?.AwaitAsync(CancellationToken.None) ?? default;
        var enqueued = false;

        try
        {
            var item = new JournalWorkItem
            {
                Kind = JournalWorkKind.Append,
                FrameBytes = frameBytes,
                FrameLength = frameLength,
                Completion = appendCompleted,
            };
            await _ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
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
            _ = Interlocked.Decrement(ref _queuedAppends);
            throw;
        }
    }

    private async ValueTask EnqueueAppendWithDurabilityAsync(byte[] frameBytes, int frameLength, JournalDurabilityWaiter durabilityWaiter, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _queuedAppends);
        var enqueued = false;
        try
        {
            var item = new JournalWorkItem
            {
                Kind = JournalWorkKind.AppendWithDurability,
                FrameBytes = frameBytes,
                FrameLength = frameLength,
                DurabilityWaiter = durabilityWaiter,
            };
            await _ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
            enqueued = true;
        }
        catch when (!enqueued)
        {
            // Only the pre-enqueue backpressure wait can fail here; once enqueued the journal thread
            // owns the counter decrement and the durability waiter completion.
            _ = Interlocked.Decrement(ref _queuedAppends);
            throw;
        }
    }

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var waiter = JournalDurabilityWaiter.Rent();
        lock (_durabilityWaitersLock)
            (_durabilityWaiters ??= []).Add(waiter);

        try
        {
            var waitTask = waiter.AwaitAsync(cancellationToken);

            // Always queue a checkpoint behind any already-enqueued appends. A !_dirty fast path
            // can complete the waiter before an in-flight append is visible on weakly-ordered CPUs.
            var item = new JournalWorkItem { Kind = JournalWorkKind.DurabilityCheckpoint };
            await _ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);

            await waitTask.ConfigureAwait(false);
            ThrowIfJournalThreadFailed();
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            RemoveDurabilityWaiter(waiter, cancellationToken);
            throw;
        }
        finally
        {
            DetachDurabilityWaiter(waiter);
            waiter.ReturnToPool();
        }
    }

    private async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = JournalDurabilityWaiter.Rent();
        try
        {
            var beginWaitTask = begin.AwaitAsync(cancellationToken);
            var beginItem = new JournalWorkItem { Kind = JournalWorkKind.MaintenanceBegin, Completion = begin };
            await _ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);

            await beginWaitTask.ConfigureAwait(false);
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = JournalRecoveryScan.DetermineNextSequence(manifest, _opt);

            var end = JournalDurabilityWaiter.Rent();
            try
            {
                var endWaitTask = end.AwaitAsync(cancellationToken);
                var endItem = new JournalWorkItem
                {
                    Kind = JournalWorkKind.MaintenanceEnd,
                    Completion = end,
                    ResetSegmentIndex = resetSegmentIndex,
                    ResetSequence = resetSequence,
                };
                await _ring.EnqueueAsync(endItem, cancellationToken).ConfigureAwait(false);

                await endWaitTask.ConfigureAwait(false);
            }
            finally
            {
                end.ReturnToPool();
            }
        }
        finally
        {
            begin.ReturnToPool();
        }
    }

    private async ValueTask EnqueueShutdownAsync()
    {
        var shutdownItem = new JournalWorkItem { Kind = JournalWorkKind.Shutdown };
        await _ring.EnqueueAsync(shutdownItem, CancellationToken.None).ConfigureAwait(false);
    }

    private void FailPendingDurabilityWaiters(Exception reason)
    {
        List<JournalDurabilityWaiter>? waiters;
        lock (_durabilityWaitersLock)
        {
            waiters = _durabilityWaiters;
            _durabilityWaiters = null;
        }

        if (waiters is null)
            return;

        foreach (var waiter in waiters)
            _ = waiter.TrySetException(reason);

        _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
    }

    private void FailJournalPipeline(Exception reason)
    {
        ArgumentNullException.ThrowIfNull(reason);
        Volatile.Write(ref _journalThreadFailure, reason);
        FailPendingDurabilityWaiters(reason);
        _groupCommit?.CancelPendingCore(reason);
    }

    private async ValueTask FlushAsync(CancellationToken cancellationToken) => await EnqueueFlushAsync(cancellationToken).ConfigureAwait(false);

    private bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
    }

    private void OnManifestRollFailed(Exception ex)
    {
        _eventLoop.MarkRollAborted();
        FailJournalPipeline(ex);
        _ring.NotifyWorkAvailable();
    }

    private void OnManifestRollSucceeded()
    {
        _eventLoop.MarkRollCompletionPending();
        _ring.NotifyWorkAvailable();
    }

    private void RemoveDurabilityWaiter(JournalDurabilityWaiter waiter, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_durabilityWaitersLock)
            removed = _durabilityWaiters?.Remove(waiter) ?? false;

        if (!removed)
            return;

        _ = waiter.TrySetCanceled(cancellationToken);
    }

    private void ThrowIfJournalThreadFailed()
    {
        if (Volatile.Read(ref _journalThreadFailure) is { } failure)
            throw new InvalidOperationException("journal I/O thread failed.", failure);
    }

    private ValueTask WaitForPendingMemoryApplyDrainAsync(CancellationToken cancellationToken)
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

    private async ValueTask WaitForSnapshotCutAdmissionAsync(CancellationToken cancellationToken)
    {
        while (true)
        {
            await WaitForPendingMemoryApplyDrainAsync(cancellationToken).ConfigureAwait(false);
            await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            if (!HasPendingMemoryApply())
                return;

            _ = _mutationGate.Release();
        }
    }
}
