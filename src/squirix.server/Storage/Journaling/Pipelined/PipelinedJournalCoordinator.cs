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
using Squirix.Server.Storage.Journaling.JsonFramed;
using Squirix.Server.Storage.Journaling.Limits;
using Squirix.Server.Storage.Journaling.Pipelined.Codec;
using Squirix.Server.Storage.Journaling.Pipelined.Platform;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.Pipelined;

/// <summary>Single-writer pipelined journal coordinator with binary frames and dedicated I/O thread.</summary>
internal sealed class PipelinedJournalCoordinator : IJournalCoordinator
{
    private const int RingCapacity = 4096;
    private readonly CancellationTokenSource _bgCts = new();
    private readonly JournalDurabilityGroupCommit? _groupCommit;
    private readonly Thread _journalThread;
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly Lock _durabilityWaitersLock = new();
    private readonly Lock _pendingMemoryApplyLock = new();
    private readonly JournalSegmentPolicy _policy;

    private readonly BoundedJournalRing _ring = new(RingCapacity);
    private readonly IJournalSegmentWriter _segmentWriter;
    private readonly Lock _sequenceLock = new();
    private readonly JournalStartupGate _startupGate;
    private string? _activeSegmentPath;
    private long _activeSegmentWrittenBytes;
    private double _avgAppendLatencyMs;
    private long _bytes;
    private volatile bool _dirty;
    private int _disposed;
    private Exception? _journalThreadFailure;
    private long _journalTotalBytes;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;
    private List<JournalDurabilityWaiter>? _durabilityWaiters;
    private int _durabilityFlushScheduled;
    private int _groupCommitCheckpointPending;
    private TaskCompletionSource? _groupCommitCheckpointTcs;
    private int _queuedAppends;

    private PipelinedJournalCoordinator(PersistenceOptions opt, Manifest manifest, ManifestStore manifestStore, JournalStartupGate startupGate, IJournalSegmentWriter segmentWriter)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _startupGate = startupGate;
        _segmentWriter = segmentWriter;
        _policy = new JournalSegmentPolicy(opt);
        _journalTotalBytes = JournalReader.GetOnDiskTotalBytes(_opt.DataDir);
        _groupCommit = _opt.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(GroupCommitFlushAsync, _opt) : null;
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

    public ulong NextSequence
    {
        get
        {
            lock (_sequenceLock)
                return _nextSequence;
        }
    }

    public double RecentAppendLatencyMs => Volatile.Read(ref _avgAppendLatencyMs);

    internal long ActiveSegmentWrittenBytes => Volatile.Read(ref _activeSegmentWrittenBytes);

    internal bool IsDurabilityFlushPending => _dirty;

    public static async Task<PipelinedJournalCoordinator> CreateAsync(
        PersistenceOptions opt,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        await PrepareActiveSegmentForSequenceScanAsync(manifest, opt, cancellationToken).ConfigureAwait(false);
        var writer = JournalSegmentWriterFactory.Create(opt.JournalPlatformBackend);
        return new PipelinedJournalCoordinator(opt, manifest, manifestStore, startupGate, writer);
    }

    public ValueTask AppendPutAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(discriminatedEntryJson);
        return AppendRecordCoreAsync(
            AllocateRecord(
                key,
                JournalOperationKind.Put,
                discriminatedEntryJson,
                operationId ?? string.Empty),
            cancellationToken);
    }

    public ValueTask AppendPutAndAwaitDurabilityAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(discriminatedEntryJson);
        if (_opt.IsJournalGroupCommitEnabled)
        {
            return AppendPutAndAwaitDurabilityViaGroupCommitAsync(key, discriminatedEntryJson, operationId, cancellationToken);
        }

        return AppendRecordWithDurabilityCoreAsync(
            AllocateRecord(
                key,
                JournalOperationKind.Put,
                discriminatedEntryJson,
                operationId ?? string.Empty),
            cancellationToken);
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
            await _groupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(PipelinedJournalCoordinator))).ConfigureAwait(false);

        FailPendingDurabilityWaiters(new ObjectDisposedException(nameof(PipelinedJournalCoordinator)));

        await EnqueueShutdownAsync().ConfigureAwait(false);
        await AwaitJournalThreadDuringDisposeAsync(failures).ConfigureAwait(false);
        await _segmentWriter.DisposeAsync().ConfigureAwait(false);
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

    public async ValueTask<TResult> ExecuteUnderSnapshotBarrierAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
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
            return await action(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _mutationGate.Release();
        }
    }

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _startupGate.WaitAsync(cancellationToken);

    internal async ValueTask FlushAsync(CancellationToken cancellationToken) => await EnqueueFlushAsync(cancellationToken).ConfigureAwait(false);

    private static void CompleteJournalWorkItem(JournalWorkItem item) => item.Completion?.SetResult();

    private static long ComputeValidLength(FileStream stream)
    {
        if (stream.Length == 0)
            return 0;

        stream.Position = 0;
        Span<byte> header = stackalloc byte[JournalBinaryFraming.FileHeaderSize];
        if (!StreamEx.TryReadExact(stream, header))
            throw new InvalidDataException("journal segment has a truncated file header.");

        JournalBinaryFraming.ValidateFileHeader(header);

        long validLength = JournalBinaryFraming.FileHeaderSize;
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

    private static ulong DetermineNextSequence(Manifest manifest, PersistenceOptions options)
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

    private static async Task PrepareActiveSegmentForSequenceScanAsync(Manifest manifest, PersistenceOptions options, CancellationToken cancellationToken)
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
        Span<byte> header = stackalloc byte[JournalBinaryFraming.FileHeaderSize];
        JournalBinaryFraming.WriteFileHeader(header);
        writer.Write(header, 0);
    }

    private ulong AllocateSequence()
    {
        lock (_sequenceLock)
            return ++_nextSequence;
    }

    private JournalRecord AllocateRecord(
        CacheKey key,
        JournalOperationKind operation,
        byte[]? putDiscriminatedEntryJson = null,
        string? putOperationId = null,
        DateTime? touchExpirationUtc = null)
    {
        var record = JournalRecord.RentForAppend();
        record.Sequence = AllocateSequence();
        record.UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        record.Operation = operation;
        record.Key = key;
        record.PutDiscriminatedEntryJson = putDiscriminatedEntryJson;
        record.PutOperationId = putOperationId;
        record.TouchExpirationUtc = touchExpirationUtc;
        return record;
    }

    private async ValueTask AppendRecordCoreAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        ThrowIfJournalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var (bodyLen, keyUtf8) = BinaryJournalCodec.PrepareEncode(record);
            var frameLen = JournalBinaryFraming.FrameTotalLength(bodyLen);
            var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
            var body = frameBytes.AsSpan(JournalBinaryFraming.FrameHeaderSize, bodyLen);
            _ = BinaryJournalCodec.Encode(record, body, keyUtf8);
            JournalBinaryFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), body);

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

    private async ValueTask AppendPutAndAwaitDurabilityViaGroupCommitAsync(
        CacheKey key,
        byte[] discriminatedEntryJson,
        string? operationId,
        CancellationToken cancellationToken)
    {
        await AppendPutAsync(key, discriminatedEntryJson, operationId, cancellationToken).ConfigureAwait(false);
        await AwaitDurabilityCommitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask AppendRecordWithDurabilityCoreAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        ThrowIfJournalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        try
        {
            var (bodyLen, keyUtf8) = BinaryJournalCodec.PrepareEncode(record);
            var frameLen = JournalBinaryFraming.FrameTotalLength(bodyLen);
            var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
            var body = frameBytes.AsSpan(JournalBinaryFraming.FrameHeaderSize, bodyLen);
            _ = BinaryJournalCodec.Encode(record, body, keyUtf8);
            JournalBinaryFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), body);

            var startedMs = Environment.TickCount64;
            var waiter = JournalDurabilityWaiter.Rent();
            try
            {
                await EnqueueAppendWithDurabilityAsync(frameBytes, frameLen, waiter, cancellationToken).ConfigureAwait(false);
                await waiter.AwaitAsync(cancellationToken).ConfigureAwait(false);
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
            if (!await Task.Run(() => _journalThread.Join(TimeSpan.FromSeconds(30)), _bgCts.Token).ConfigureAwait(false))
            {
                failures.Add(new TimeoutException("journal I/O thread did not exit within 30 seconds."));
            }
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // Dispose cancelled the join wait when teardown already completed.
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
    }

    private async ValueTask EnqueueAppendAsync(byte[] frameBytes, int frameLength, CancellationToken cancellationToken)
    {
        _ = Interlocked.Increment(ref _queuedAppends);
        var appendCompleted = _opt.IsJournalGroupCommitEnabled ? JournalDurabilityWaiter.Rent() : null;
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
                await appendCompleted.AwaitAsync(cancellationToken).ConfigureAwait(false);
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

    private async ValueTask EnqueueAppendWithDurabilityAsync(
        byte[] frameBytes,
        int frameLength,
        JournalDurabilityWaiter durabilityWaiter,
        CancellationToken cancellationToken)
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

            if (Volatile.Read(ref _queuedAppends) > 0)
            {
                await waitTask.ConfigureAwait(false);
                return;
            }

            if (!_dirty)
            {
                CompleteDurabilityWaiterImmediately(waiter);
                await waitTask.ConfigureAwait(false);
                return;
            }

            if (Interlocked.CompareExchange(ref _durabilityFlushScheduled, 1, 0) is 0)
            {
                var item = new JournalWorkItem { Kind = JournalWorkKind.DurabilityCheckpoint };
                await _ring.EnqueueAsync(item, cancellationToken).ConfigureAwait(false);
            }

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
            waiter.ReturnToPool();
        }
    }

    private void CompleteDurabilityWaiterImmediately(JournalDurabilityWaiter waiter)
    {
        lock (_durabilityWaitersLock)
            _ = _durabilityWaiters?.Remove(waiter);

        waiter.SetResult();
    }

    private async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = JournalDurabilityWaiter.Rent();
        try
        {
            var beginItem = new JournalWorkItem { Kind = JournalWorkKind.MaintenanceBegin, Completion = begin };
            await _ring.EnqueueAsync(beginItem, cancellationToken).ConfigureAwait(false);

            await begin.AwaitAsync(cancellationToken).ConfigureAwait(false);
            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            var resetSequence = DetermineNextSequence(manifest, _opt);

            var end = JournalDurabilityWaiter.Rent();
            try
            {
                var endItem = new JournalWorkItem
                {
                    Kind = JournalWorkKind.MaintenanceEnd,
                    Completion = end,
                    ResetSegmentIndex = resetSegmentIndex,
                    ResetSequence = resetSequence,
                };
                await _ring.EnqueueAsync(endItem, cancellationToken).ConfigureAwait(false);

                await end.AwaitAsync(cancellationToken).ConfigureAwait(false);
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
            return;

        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        var append = File.Exists(_activeSegmentPath);
        _segmentWriter.OpenSegment(_activeSegmentPath, append);
        if (_segmentWriter.Length == 0)
        {
            Span<byte> header = stackalloc byte[JournalBinaryFraming.FileHeaderSize];
            JournalBinaryFraming.WriteFileHeader(header);
            _segmentWriter.Write(header, 0);
        }

        _activeSegmentWrittenBytes = _segmentWriter.Length;
    }

    private async ValueTask GroupCommitFlushAsync(CancellationToken cancellationToken)
    {
        if (!_dirty)
            return;

        if (Volatile.Read(ref _queuedAppends) > 0)
        {
            _ = Interlocked.Exchange(ref _groupCommitCheckpointPending, 1);
            var checkpoint = Volatile.Read(ref _groupCommitCheckpointTcs);
            if (checkpoint is null || checkpoint.Task.IsCompleted)
            {
                var created = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
                var existing = Interlocked.CompareExchange(ref _groupCommitCheckpointTcs, created, checkpoint);
                checkpoint = existing ?? created;
            }

            await checkpoint.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        await EnqueueFlushAsync(cancellationToken).ConfigureAwait(false);
    }

    private void FsyncOnJournalThread()
    {
        if (!_dirty)
            return;

        _segmentWriter.Fsync();
        _dirty = false;
    }

    private bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
    }

    private void JournalThreadMain()
    {
        try
        {
            while (true)
            {
                if (!_ring.TryDequeue(out var item))
                {
                    _ring.SpinWaitForWork(_bgCts.Token);
                    if (!_ring.TryDequeue(out item))
                        continue;
                }

                switch (item.Kind)
                {
                    case JournalWorkKind.Append:
                        ProcessAppend(item);
                        break;

                    case JournalWorkKind.AppendWithDurability:
                        ProcessAppendWithDurability(item);
                        break;

                    case JournalWorkKind.Flush:
                    case JournalWorkKind.DurabilityCheckpoint:
                        CompleteDurabilityCheckpointOnJournalThread();
                        break;

                    case JournalWorkKind.Shutdown:
                        FsyncOnJournalThread();
                        return;

                    case JournalWorkKind.MaintenanceBegin:
                        FsyncOnJournalThread();
                        _activeSegmentPath = null;
                        CompleteJournalWorkItem(item);
                        break;

                    case JournalWorkKind.MaintenanceEnd:
                        CurrentSegmentIndex = item.ResetSegmentIndex;
                        lock (_sequenceLock)
                            _nextSequence = item.ResetSequence;
                        _activeSegmentWrittenBytes = 0;
                        _dirty = false;
                        CompleteJournalWorkItem(item);
                        break;

                    default:
                        throw new InvalidOperationException($"unknown journal work kind {item.Kind}.");
                }
            }
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            Volatile.Write(ref _journalThreadFailure, ex);
            FailPendingDurabilityWaiters(ex);
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // journal I/O thread exits when background cancellation is requested during dispose.
        }
    }

    private void MaybeRollSegment(int incomingFrameBytes)
    {
        if (!_policy.ShouldRollSegment(_activeSegmentWrittenBytes, incomingFrameBytes))
            return;

        JournalReadPath.EnsureSegmentRollCapacityOrThrow(_opt.DataDir, _policy);
        RollSegmentOnJournalThread();
    }

    private void ProcessAppendWithDurability(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes;
        var waiter = item.DurabilityWaiter ?? throw new InvalidOperationException("AppendWithDurability work item is missing a durability waiter.");
        try
        {
            WriteAppendFrame(item);
            FsyncOnJournalThread();
            waiter.SetResult();
        }
        finally
        {
            _ = Interlocked.Decrement(ref _queuedAppends);
            if (frameBytes is not null)
                ArrayPool<byte>.Shared.Return(frameBytes);
        }
    }

    private void WriteAppendFrame(JournalWorkItem item)
    {
        var frameBytes = item.FrameBytes ?? throw new InvalidOperationException("Append work item is missing frame bytes.");
        EnsureSegmentOpen();
        _policy.EnsureAppendCapacityOrThrow(_journalTotalBytes, item.FrameLength);
        MaybeRollSegment(item.FrameLength);
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

        if (!_opt.IsJournalGroupCommitEnabled)
            CompleteDurabilityCheckpointOnJournalThread();
        else
            TryCompleteGroupCommitCheckpoint();

        CompleteJournalWorkItem(item);
    }

    private void TryCompleteGroupCommitCheckpoint()
    {
        if (Volatile.Read(ref _groupCommitCheckpointPending) is 0)
            return;

        if (Volatile.Read(ref _queuedAppends) > 0)
            return;

        if (_dirty)
            FsyncOnJournalThread();

        _ = Interlocked.Exchange(ref _groupCommitCheckpointPending, 0);
        var checkpoint = Interlocked.Exchange(ref _groupCommitCheckpointTcs, null);
        if (checkpoint is not null)
            _ = checkpoint.TrySetResult();
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
            waiter.SetResult();

        _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
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
            waiter.SetException(reason);

        _ = Interlocked.Exchange(ref _durabilityFlushScheduled, 0);
        _ = Interlocked.Exchange(ref _groupCommitCheckpointPending, 0);
        var checkpoint = Interlocked.Exchange(ref _groupCommitCheckpointTcs, null);
        if (checkpoint is not null)
            _ = checkpoint.TrySetException(reason);
    }

    private void RemoveDurabilityWaiter(JournalDurabilityWaiter waiter, CancellationToken cancellationToken)
    {
        bool removed;
        lock (_durabilityWaitersLock)
            removed = _durabilityWaiters?.Remove(waiter) ?? false;

        if (!removed)
            return;

        waiter.SetCanceled(cancellationToken);
    }

    private void RollSegmentOnJournalThread()
    {
        FsyncOnJournalThread();
        CurrentSegmentIndex++;
        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        _segmentWriter.OpenSegment(_activeSegmentPath, false);
        Span<byte> header = stackalloc byte[JournalBinaryFraming.FileHeaderSize];
        JournalBinaryFraming.WriteFileHeader(header);
        _segmentWriter.Write(header, 0);
        Volatile.Write(ref _activeSegmentWrittenBytes, JournalBinaryFraming.FileHeaderSize);
        _journalTotalBytes += JournalBinaryFraming.FileHeaderSize;
        _dirty = false;
        WriteManifestAfterRollSync();
    }

    private void WriteManifestAfterRollSync()
    {
        ulong nextSequence;
        lock (_sequenceLock)
            nextSequence = _nextSequence;

        var prevManifest = _manifestStore.ReadCurrentOrDefaultBlocking();
        var manifest = new Manifest
        {
            Format = prevManifest.Format is 0 ? 1 : prevManifest.Format,
            CurrentJournal = CurrentSegmentIndex,
            NextSequence = nextSequence,
            LastSnapshot = prevManifest.LastSnapshot,
        };
        _manifestStore.WriteBlocking(manifest);
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
