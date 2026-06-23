using System;
using System.Buffers;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.Storage.Journaling.Platform;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Single-writer pipelined journal coordinator with binary frames (see docs/journal-binary-format.md).</summary>
internal sealed class JournalCoordinator : IJournalCoordinator
{
    private const int RingCapacity = 4096;
    private readonly CancellationTokenSource _bgCts = new();
    private readonly Lock _durabilityWaitersLock = new();
    private readonly JournalDurabilityGroupCommit? _groupCommit;
    private readonly Thread _journalThread;
    private readonly ManifestRollPublisher _manifestRollPublisher;
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly Lock _pendingMemoryApplyLock = new();
    private readonly JournalSegmentPolicy _policy;

    private readonly BoundedJournalRing _ring = new(RingCapacity);
    private readonly IJournalSegmentWriter _segmentWriter;
    private readonly JournalStartupGate _startupGate;
    private readonly JournalWriteBatchBuffer _writeBatch = new();
    private string? _activeSegmentPath;
    private long _activeSegmentWrittenBytes;
    private double _avgAppendLatencyMs;
    private long _bytes;
    private volatile bool _dirty;
    private int _disposed;
    private int _durabilityFlushScheduled;
    private List<JournalDurabilityWaiter>? _durabilityWaiters;
    private Exception? _journalThreadFailure;
    private long _journalTotalBytes;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;
    private int _pendingRollTargetSegmentIndex;
    private int _queuedAppends;
    private int _segmentRollCompletionPending;
    private bool _segmentRollInFlight;

    private JournalCoordinator(PersistenceOptions opt, ManifestState manifest, ManifestStore manifestStore, JournalStartupGate startupGate)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _startupGate = startupGate;
        _manifestRollPublisher = new ManifestRollPublisher(manifestStore, OnManifestRollFailed);
        _segmentWriter = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        _policy = new JournalSegmentPolicy(opt);
        _journalTotalBytes = JournalReader.GetOnDiskTotalBytes(_opt.DataDir);
        _groupCommit = _opt.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(FlushGroupCommitOnJournalThread, () => _ring.NotifyWorkAvailable(), _opt) : null;
        _ = DirectoryEx.CreateDirectory(_opt.DataDir);
        CurrentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        _nextSequence = DetermineNextSequence(manifest, _opt);
        _journalThread = new Thread(JournalThreadMain) { IsBackground = true, Name = "squirix-journal-io" };
        _journalThread.Start();
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex { get; private set; }

    public bool HasFlushLoopFailure => Volatile.Read(ref _journalThreadFailure) is not null;

    public bool IsJournalGroupCommitEnabled => _opt.IsJournalGroupCommitEnabled;

    public ulong NextSequence => Volatile.Read(ref _nextSequence);

    public double RecentAppendLatencyMs => Volatile.Read(ref _avgAppendLatencyMs);

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    internal bool IsDurabilityFlushPending => _dirty;

    public static async Task<JournalCoordinator> CreateAsync(
        PersistenceOptions opt,
        ManifestState manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        await PrepareActiveSegmentForSequenceScanAsync(manifest, opt, cancellationToken).ConfigureAwait(false);
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

    private static void CompleteJournalWorkItem(JournalWorkItem item) => item.Completion?.SetResult();

    private static long ComputeValidLength(FileStream stream)
    {
        if (stream.Length == 0)
            return 0;

        stream.Position = 0;
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        if (!StreamEx.TryReadExact(stream, header))
            throw new InvalidDataException("journal segment has a truncated file header.");

        JournalFraming.EnsureSegmentHeaderSupported(header);

        long validLength = JournalFraming.FileHeaderSize;
        while (true)
        {
            var read = JournalFrameReader.ReadNext(stream, validLength, out var rentedBuffer, out _);
            if (read.Status is JournalFrameReadStatus.EndOfFile or not JournalFrameReadStatus.Success)
                return validLength;

            validLength = read.NextFrameOffset;
            if (rentedBuffer is not null)
                ArrayPool<byte>.Shared.Return(rentedBuffer);
        }
    }

    private static InvalidDataException CreateJournalTopologyDisjointForSequenceInit(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment) => new(
        $"journal recovery cannot determine a valid replay start. manifestCurrentJournal={manifestCurrentJournal.ToString(CultureInfo.InvariantCulture)}, firstAvailableJournal={(firstAvailableSegment > 0 ? firstAvailableSegment : 0).ToString(CultureInfo.InvariantCulture)}, lastAvailableJournal={(lastAvailableSegment > 0 ? lastAvailableSegment : 0).ToString(CultureInfo.InvariantCulture)}, chosenReplayStartSegment=0, snapshotPresent=False.");

    private static ulong DetermineNextSequence(ManifestState manifest, PersistenceOptions options)
    {
        var next = manifest.NextSequence is 0UL ? 1UL : manifest.NextSequence;
        if (manifest.LastSnapshot?.LastAppliedSequence is { } lastApplied && lastApplied >= next)
            next = lastApplied + 1UL;

        var manifestCurrentJournal = manifest.CurrentJournal > 0 ? manifest.CurrentJournal : 1;
        var firstAvailableSegment = 0;
        var lastAvailableSegment = 0;
        foreach (var segment in JournalReadPath.EnumerateSegments(options.DataDir, 1))
        {
            if (firstAvailableSegment is 0)
                firstAvailableSegment = segment.Index;

            lastAvailableSegment = segment.Index;
        }

        ThrowIfJournalOnlyTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);
        var scanStartSegment = firstAvailableSegment is 0 ? 1 : Math.Max(firstAvailableSegment, manifestCurrentJournal);

        foreach (var record in JournalReadPath.ReadAll(options.DataDir, scanStartSegment, CancellationToken.None))
        {
            if (record.Sequence >= next)
                next = record.Sequence + 1UL;
        }

        return next;
    }

    private static async Task PrepareActiveSegmentForSequenceScanAsync(ManifestState manifest, PersistenceOptions options, CancellationToken cancellationToken)
    {
        var segmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var path = JournalReadPath.BuildSegmentPath(options.DataDir, segmentIndex);
        if (!File.Exists(path))
            return;

        var writer = JournalSegmentWriterFactory.Create(options.JournalPlatformBackend);
        await using (writer.ConfigureAwait(false))
        {
            writer.OpenSegment(path, true);
            if (writer.Length == 0)
                return;

            await RepairTornTailIfNeededAsync(writer, path, cancellationToken).ConfigureAwait(false);
        }

        cancellationToken.ThrowIfCancellationRequested();
    }

    private static async Task<long> ReadValidSegmentLengthAsync(string path, CancellationToken cancellationToken)
    {
        var stream = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite | FileShare.Delete);
        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            return ComputeValidLength(stream);
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task RepairTornTailIfNeededAsync(IJournalSegmentWriter writer, string path, CancellationToken cancellationToken)
    {
        try
        {
            var validLength = await ReadValidSegmentLengthAsync(path, cancellationToken).ConfigureAwait(false);
            if (validLength == writer.Length)
                return;

            writer.Truncate(validLength);
            if (validLength == 0)
                WriteFreshFileHeader(writer);

            writer.Fsync();
        }
        catch (InvalidDataException) when (writer.Length > 0)
        {
            writer.Truncate(0);
            WriteFreshFileHeader(writer);
            writer.Fsync();
        }
    }

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

    private static void ThrowIfJournalOnlyTopologyDisjointForSequenceInit(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment)
    {
        if (firstAvailableSegment is 0)
        {
            if (manifestCurrentJournal is not 1)
                throw CreateJournalTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);

            return;
        }

        if (lastAvailableSegment < manifestCurrentJournal)
            throw CreateJournalTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);
    }

    private static void WriteFreshFileHeader(IJournalSegmentWriter writer)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);
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
                var waitTask = waiter.AwaitAsync(cancellationToken);
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

        if (_dirty)
            FsyncOnJournalThread();

        foreach (var waiter in waiters)
            _ = waiter.TrySetResult();

        _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
    }

    private void CompleteStagedAppend(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            _ = Interlocked.Decrement(ref _queuedAppends);
        }
        finally
        {
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }

        CompleteJournalWorkItem(item);
    }

    private void DetachDurabilityWaiter(JournalDurabilityWaiter waiter)
    {
        lock (_durabilityWaitersLock)
            _ = _durabilityWaiters?.Remove(waiter);
    }

    private void DrainDueGroupCommitBatches()
    {
        if (_groupCommit is null || Volatile.Read(ref _queuedAppends) > 0)
            return;

        _groupCommit.DrainDueBatchesOnJournalThread();
    }

    private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _queuedAppends);
        var appendCompleted = _opt.IsJournalGroupCommitEnabled ? JournalDurabilityWaiter.Rent() : null;
        ValueTask appendWaitTask = default;
        if (appendCompleted is not null)
            appendWaitTask = appendCompleted.AwaitAsync(cancellationToken);

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

            if (appendCompleted is not null)
            {
                await appendWaitTask.ConfigureAwait(false);
                appendCompleted.ReturnToPool();
            }

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
            appendCompleted?.ReturnToPool();
            _ = Interlocked.Decrement(ref _queuedAppends);
            throw;
        }
    }

    private async ValueTask EnqueueAppendWithDurabilityAsync(byte[] frameBytes, int frameLength, JournalDurabilityWaiter durabilityWaiter, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _queuedAppends);
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

            cancellationToken.ThrowIfCancellationRequested();
        }
        catch
        {
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
            var resetSequence = DetermineNextSequence(manifest, _opt);

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

    private void EnsureSegmentOpen()
    {
        if (_activeSegmentPath is not null)
        {
            SyncActiveSegmentBytesToWriterLength();
            return;
        }

        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        var append = File.Exists(_activeSegmentPath);
        _segmentWriter.OpenSegment(_activeSegmentPath, append);
        if (_segmentWriter.Length == 0)
        {
            Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
            JournalFraming.WriteFileHeader(header);
            _segmentWriter.Write(header, 0);
        }

        _activeSegmentWrittenBytes = _segmentWriter.Length;
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

    private void FlushGroupCommitOnJournalThread()
    {
        FlushWriteBatch();
        if (_dirty)
            FsyncOnJournalThread();
    }

    private void FlushWriteBatch()
    {
        if (_writeBatch.IsEmpty)
            return;

        var span = _writeBatch.ActiveSpan;
        SyncActiveSegmentBytesToWriterLength();
        var offset = _activeSegmentWrittenBytes;
        try
        {
            _segmentWriter.Write(span, offset);
        }
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }

        Volatile.Write(ref _activeSegmentWrittenBytes, offset + span.Length);
        _journalTotalBytes += span.Length;
        _dirty = true;

        foreach (var pending in _writeBatch.PendingAppends)
            CompleteStagedAppend(pending.Item);

        _writeBatch.Clear();

        if (_opt.IsJournalGroupCommitEnabled)
            TryCompleteGroupCommitCheckpoint();
    }

    private void FsyncOnJournalThread()
    {
        if (!_dirty)
            return;

        _segmentWriter.Fsync();
        _dirty = false;
    }

    private long GetEffectiveActiveSegmentBytes() => _activeSegmentWrittenBytes + _writeBatch.StagedByteLength;

    private bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
    }

    private void JournalThreadMain()
    {
        try
        {
            JournalWorkItem? rollDeferredAppend = null;
            for (var running = true; running;)
                running = RunJournalThreadIteration(ref rollDeferredAppend);
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException or TimeoutException)
        {
            FailJournalPipeline(ex);
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // journal I/O thread exits when background cancellation is requested during dispose.
        }
    }

    private bool DrainJournalRing(ref JournalWorkItem? rollDeferredAppend, out bool shutdownRequested)
    {
        shutdownRequested = false;
        var hadWork = false;
        while (_ring.TryDequeue(out var item))
        {
            hadWork = true;
            if (item.Kind is JournalWorkKind.Append)
            {
                if (TryAcceptAppendIntoBatch(item, out var rollDeferred))
                    continue;

                if (rollDeferred)
                {
                    rollDeferredAppend = item;
                    return hadWork;
                }

                FlushWriteBatch();
                _ = ProcessJournalWorkItem(item);
                continue;
            }

            FlushWriteBatch();
            if (!ProcessJournalWorkItem(item))
                continue;

            FlushWriteBatch();
            shutdownRequested = true;
            return hadWork;
        }

        return hadWork;
    }

    private bool RunJournalThreadIteration(ref JournalWorkItem? rollDeferredAppend)
    {
        if (TryCompletePendingSegmentRoll() && rollDeferredAppend is not null)
        {
            ProcessRollDeferredAppend(ref rollDeferredAppend);
            return true;
        }

        if (rollDeferredAppend is not null)
        {
            ThrowIfJournalThreadFailed();
            _ring.WaitForWork(Timeout.Infinite, _bgCts.Token);
            return true;
        }

        var hadWork = DrainJournalRing(ref rollDeferredAppend, out var shutdownRequested);
        if (shutdownRequested)
            return false;

        if (rollDeferredAppend is not null)
            return true;

        FlushWriteBatch();
        DrainDueGroupCommitBatches();

        if (hadWork)
            return true;

        var timeoutMs = Volatile.Read(ref _queuedAppends) > 0 ? Timeout.Infinite : _groupCommit?.GetJournalThreadWaitTimeoutMs() ?? Timeout.Infinite;
        _ring.WaitForWork(timeoutMs, _bgCts.Token);
        DrainDueGroupCommitBatches();
        return true;
    }

    private void BeginSegmentRollOnJournalThread()
    {
        if (_segmentRollInFlight)
            return;

        FsyncOnJournalThread();
        JournalReadPath.EnsureSegmentRollCapacityOrThrow(_opt.DataDir, _policy);
        _pendingRollTargetSegmentIndex = CurrentSegmentIndex + 1;
        _segmentRollInFlight = true;
        _manifestRollPublisher.PublishRoll(
            _pendingRollTargetSegmentIndex,
            Volatile.Read(ref _nextSequence),
            OnManifestRollSucceeded);
    }

    private void CompleteSegmentRollOnJournalThread()
    {
        CurrentSegmentIndex = _pendingRollTargetSegmentIndex;
        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        _segmentWriter.OpenSegment(_activeSegmentPath, false);
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        _segmentWriter.Write(header, 0);
        Volatile.Write(ref _activeSegmentWrittenBytes, JournalFraming.FileHeaderSize);
        _journalTotalBytes += JournalFraming.FileHeaderSize;
        _dirty = false;
        _segmentRollInFlight = false;
    }

    private void OnManifestRollFailed(Exception ex)
    {
        _segmentRollInFlight = false;
        Volatile.Write(ref _segmentRollCompletionPending, 0);
        FailJournalPipeline(ex);
        _ring.NotifyWorkAvailable();
    }

    private void OnManifestRollSucceeded()
    {
        Volatile.Write(ref _segmentRollCompletionPending, 1);
        _ring.NotifyWorkAvailable();
    }

    private void ProcessRollDeferredAppend(ref JournalWorkItem? rollDeferredAppend)
    {
        var item = rollDeferredAppend ?? throw new InvalidOperationException("roll-deferred append is missing.");
        rollDeferredAppend = null;
        if (TryAcceptAppendIntoBatch(item, out var rollDeferred))
            return;

        if (rollDeferred)
        {
            rollDeferredAppend = item;
            return;
        }

        FlushWriteBatch();
        _ = ProcessJournalWorkItem(item);
    }

    private bool TryCompletePendingSegmentRoll()
    {
        if (Volatile.Read(ref _segmentRollCompletionPending) is 0)
            return false;

        Volatile.Write(ref _segmentRollCompletionPending, 0);
        CompleteSegmentRollOnJournalThread();
        return true;
    }

    private void ProcessAppend(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        try
        {
            WriteAppendFrame(item);
        }
        finally
        {
            _ = Interlocked.Decrement(ref _queuedAppends);
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }

        if (_opt.IsJournalGroupCommitEnabled)
            TryCompleteGroupCommitCheckpoint();

        CompleteJournalWorkItem(item);
    }

    private void ProcessAppendWithDurability(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        var waiter = item.DurabilityWaiter ?? throw new InvalidOperationException("AppendWithDurability work item is missing a durability waiter.");
        try
        {
            WriteAppendFrame(item);
            FsyncOnJournalThread();
            _ = waiter.TrySetResult();
        }
        finally
        {
            _ = Interlocked.Decrement(ref _queuedAppends);
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }
    }

    private bool ProcessJournalWorkItem(JournalWorkItem item)
    {
        switch (item.Kind)
        {
            case JournalWorkKind.Append:
                ProcessAppend(item);
                return false;

            case JournalWorkKind.AppendWithDurability:
                ProcessAppendWithDurability(item);
                return false;

            case JournalWorkKind.Flush:
            case JournalWorkKind.DurabilityCheckpoint:
                FlushWriteBatch();
                CompleteDurabilityCheckpointOnJournalThread();
                return false;

            case JournalWorkKind.Shutdown:
                FlushWriteBatch();
                FsyncOnJournalThread();
                return true;

            case JournalWorkKind.MaintenanceBegin:
                FlushWriteBatch();
                FsyncOnJournalThread();
                _activeSegmentPath = null;
                CompleteJournalWorkItem(item);
                return false;

            case JournalWorkKind.MaintenanceEnd:
                CurrentSegmentIndex = item.ResetSegmentIndex;
                Volatile.Write(ref _nextSequence, item.ResetSequence);
                _activeSegmentWrittenBytes = 0;
                _dirty = false;
                CompleteJournalWorkItem(item);
                return false;

            default:
                throw new InvalidOperationException($"unknown journal work kind {item.Kind}.");
        }
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

    private bool ShouldRollSegmentForAppend(int incomingFrameBytes) => _policy.ShouldRollSegment(GetEffectiveActiveSegmentBytes(), incomingFrameBytes);

    private void SyncActiveSegmentBytesToWriterLength()
    {
        if (_activeSegmentPath is null)
            return;

        var length = _segmentWriter.Length;
        if (length > _activeSegmentWrittenBytes)
            Volatile.Write(ref _activeSegmentWrittenBytes, length);
    }

    private void ThrowIfJournalThreadFailed()
    {
        if (Volatile.Read(ref _journalThreadFailure) is { } failure)
            throw new InvalidOperationException("journal I/O thread failed.", failure);
    }

    private void TruncateActiveSegmentAfterFailedFrame(long frameStart)
    {
        _segmentWriter.Truncate(frameStart);
        Volatile.Write(ref _activeSegmentWrittenBytes, frameStart);
        _dirty = frameStart > 0;
    }

    private bool TryAcceptAppendIntoBatch(JournalWorkItem item, out bool rollDeferred)
    {
        rollDeferred = false;
        EnsureSegmentOpen();
        _policy.EnsureAppendCapacityOrThrow(_journalTotalBytes, item.FrameLength);
        if (ShouldRollSegmentForAppend(item.FrameLength))
        {
            FlushWriteBatch();
            BeginSegmentRollOnJournalThread();
            rollDeferred = true;
            return false;
        }

        if (_writeBatch.TryStageAppend(item))
            return true;

        FlushWriteBatch();
        return _writeBatch.TryStageAppend(item);
    }

    private void TryCompleteGroupCommitCheckpoint() => DrainDueGroupCommitBatches();

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

    private void WriteAppendFrame(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        EnsureSegmentOpen();
        _policy.EnsureAppendCapacityOrThrow(_journalTotalBytes, item.FrameLength);
        SyncActiveSegmentBytesToWriterLength();
        if (ShouldRollSegmentForAppend(item.FrameLength))
            throw new InvalidOperationException("append requires a segment roll; use the journal thread deferral path.");
        var offset = _activeSegmentWrittenBytes;
        try
        {
            _segmentWriter.Write(frameBytes.AsSpan(0, item.FrameLength), offset);
        }
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(offset);
            throw;
        }

        Volatile.Write(ref _activeSegmentWrittenBytes, offset + item.FrameLength);
        _journalTotalBytes += item.FrameLength;
        _dirty = true;
    }
}
