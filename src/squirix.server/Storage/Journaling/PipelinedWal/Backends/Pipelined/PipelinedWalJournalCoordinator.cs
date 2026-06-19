using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.PipelinedWal.Codec;
using Squirix.Server.Storage.Journaling.PipelinedWal.Limits;
using Squirix.Server.Storage.Journaling.PipelinedWal.Platform;
using Squirix.Server.Storage.Journaling.PipelinedWal.Read;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling.PipelinedWal.Backends.Pipelined;

/// <summary>Single-writer pipelined WAL coordinator with binary frames and dedicated I/O thread.</summary>
internal sealed class PipelinedWalJournalCoordinator : IJournalCoordinator
{
    private const int RingCapacity = 4096;
    private const int BatchFlushBytes = 4 * 1024 * 1024;
    private const int BatchFlushIntervalMs = 1;

    private readonly BoundedMpscRing _ring = new(RingCapacity);
    private readonly CancellationTokenSource _bgCts = new();
    private readonly JournalDurabilityGroupCommit? _groupCommit;
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly Lock _pendingMemoryApplyLock = new();
    private readonly Lock _sequenceLock = new();
    private readonly WalSegmentPolicy _policy;
    private readonly JournalStartupGate _startupGate;
    private readonly IWalSegmentWriter _segmentWriter;
    private readonly Thread _walThread;
    private double _avgAppendLatencyMs;
    private long _bytes;
    private int _disposed;
    private Exception? _walThreadFailure;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;
    private long _activeSegmentWrittenBytes;
    private long _batchBytesSinceFlush;
    private long _lastFlushTimestampMs;
    private volatile bool _dirty;
    private string? _activeSegmentPath;

    private PipelinedWalJournalCoordinator(
        PersistenceOptions opt,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        IWalSegmentWriter segmentWriter)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _startupGate = startupGate;
        _segmentWriter = segmentWriter;
        _policy = new WalSegmentPolicy(opt);
        _groupCommit = _opt.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(FlushAsync, _opt) : null;
        _ = DirectoryEx.CreateDirectory(_opt.DataDir);
        CurrentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        _nextSequence = DetermineNextSequence(manifest, _opt);
        _walThread = new Thread(WalThreadMain) { IsBackground = true, Name = "squirix-wal" };
        _walThread.Start();
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex { get; private set; }

    public bool HasFlushLoopFailure => Volatile.Read(ref _walThreadFailure) is not null;

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

    internal long ActiveSegmentWrittenBytes => Interlocked.Read(ref _activeSegmentWrittenBytes);

    internal bool IsDurabilityFlushPending => _dirty;

    public static async Task<PipelinedWalJournalCoordinator> CreateAsync(
        PersistenceOptions opt,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        await PrepareActiveSegmentForSequenceScanAsync(manifest, opt, cancellationToken).ConfigureAwait(false);
        var writer = WalSegmentWriterFactory.Create(opt.WalPlatformBackend);
        return new PipelinedWalJournalCoordinator(opt, manifest, manifestStore, startupGate, writer);
    }

    public ValueTask AppendPutAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(discriminatedEntryJson);
        return AppendRecordAsync(
            new JournalRecord
            {
                Sequence = AllocateSequence(),
                UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                Operation = JournalOperationKind.Put,
                Key = key,
                PutDiscriminatedEntryJson = discriminatedEntryJson,
                PutOperationId = operationId ?? string.Empty,
            },
            cancellationToken);
    }

    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => AppendRecordAsync(
        CreateRecord(key, JournalOperationKind.Remove),
        cancellationToken);

    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => AppendRecordAsync(
        CreateRecord(key, JournalOperationKind.RemoveExpiration),
        cancellationToken);

    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => AppendRecordAsync(
        new JournalRecord
        {
            Sequence = AllocateSequence(),
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Operation = JournalOperationKind.TouchExpiration,
            Key = key,
            TouchExpirationUtc = expiresUtc,
        },
        cancellationToken);

    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        ThrowIfWalThreadFailed();
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
            await _groupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(PipelinedWalJournalCoordinator))).ConfigureAwait(false);

        EnqueueShutdown();
        await AwaitWalThreadDuringDisposeAsync(failures).ConfigureAwait(false);
        await _segmentWriter.DisposeAsync().ConfigureAwait(false);
        _bgCts.Dispose();
        _mutationGate.Dispose();
        ThrowDisposeFailures(failures);
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        ThrowIfWalThreadFailed();
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
        ThrowIfWalThreadFailed();

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
        ThrowIfWalThreadFailed();

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

    public ValueTask FlushAsync(CancellationToken cancellationToken) => EnqueueFlushAsync(cancellationToken);

    public ValueTask WaitForStartupAsync(CancellationToken cancellationToken) => _startupGate.WaitAsync(cancellationToken);

    private static JournalRecord CreateRecord(CacheKey key, JournalOperationKind operation) => new()
    {
        Sequence = 0,
        UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
        Operation = operation,
        Key = key,
    };

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

        var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite | FileShare.Delete);
        await using (stream.ConfigureAwait(false))
        {
            if (stream.Length == 0)
                return;

            try
            {
                var validLength = ComputeValidLength(stream);
                if (validLength == stream.Length)
                    return;

                stream.SetLength(validLength);
                if (validLength == 0)
                {
                    var header = new byte[WalBinaryFraming.FileHeaderSize];
                    WalBinaryFraming.WriteFileHeader(header);
                    await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
                }

                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (InvalidDataException) when (stream.Length > 0)
            {
                stream.SetLength(0);
                var header = new byte[WalBinaryFraming.FileHeaderSize];
                WalBinaryFraming.WriteFileHeader(header);
                await stream.WriteAsync(header, cancellationToken).ConfigureAwait(false);
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
    }

    private static long ComputeValidLength(FileStream stream)
    {
        if (stream.Length == 0)
            return 0;

        stream.Position = 0;
        Span<byte> header = stackalloc byte[WalBinaryFraming.FileHeaderSize];
        if (!StreamEx.TryReadExact(stream, header))
            throw new InvalidDataException("journal segment has a truncated file header.");

        WalBinaryFraming.ValidateFileHeader(header);

        long validLength = WalBinaryFraming.FileHeaderSize;
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

    private static void CompleteWalWorkItem(WalWorkItem item)
    {
        item.Completion?.SetResult();
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

    private ulong AllocateSequence()
    {
        lock (_sequenceLock)
            return ++_nextSequence;
    }

    private ValueTask AppendRecordAsync(JournalRecord template, CancellationToken cancellationToken)
    {
        ThrowIfWalThreadFailed();
        var record = template.Sequence is 0
            ? new JournalRecord
            {
                Sequence = AllocateSequence(),
                UnixMs = template.UnixMs,
                Operation = template.Operation,
                Key = template.Key,
                PutDiscriminatedEntryJson = template.PutDiscriminatedEntryJson,
                PutOperationId = template.PutOperationId,
                TouchExpirationUtc = template.TouchExpirationUtc,
            }
            : template;

        return AppendRecordCoreAsync(record, cancellationToken);
    }

    private async ValueTask AppendRecordCoreAsync(JournalRecord record, CancellationToken cancellationToken)
    {
        ThrowIfWalThreadFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);

        var bodyLen = BinaryWalJournalCodec.ComputeFrameBodyLength(record);
        var frameLen = WalBinaryFraming.FrameTotalLength(bodyLen);
        var frameBytes = ArrayPool<byte>.Shared.Rent(frameLen);
        try
        {
            var body = frameBytes.AsSpan(WalBinaryFraming.FrameHeaderSize, bodyLen);
            _ = BinaryWalJournalCodec.Instance.Encode(record, body);
            WalBinaryFraming.WriteFrame(frameBytes.AsSpan(0, frameLen), body);

            var sw = Stopwatch.StartNew();
            await EnqueueAppendAsync(frameBytes, frameLen, cancellationToken).ConfigureAwait(false);
            sw.Stop();

            var elapsedMs = sw.Elapsed.TotalMilliseconds;
            var currentLatency = Volatile.Read(ref _avgAppendLatencyMs);
            Volatile.Write(ref _avgAppendLatencyMs, currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1));

            _ = Interlocked.Add(ref _bytes, frameLen);
            _ = Interlocked.Increment(ref _ops);
            OnAppended?.Invoke(this, EventArgs.Empty);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(frameBytes);
        }
    }

    private async ValueTask AwaitWalThreadDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
            if (!await Task.Run(() => _walThread.Join(TimeSpan.FromSeconds(30)), _bgCts.Token).ConfigureAwait(false))
            {
                failures.Add(new TimeoutException("WAL thread did not exit within 30 seconds."));
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
        var copy = new byte[frameLength];
        Buffer.BlockCopy(frameBytes, 0, copy, 0, frameLength);
        var item = new WalWorkItem { Kind = WalWorkKind.Append, FrameBytes = copy, FrameLength = frameLength };
        while (!_ring.TryEnqueue(in item))
            await Task.Yield();

        _dirty = true;
        cancellationToken.ThrowIfCancellationRequested();
    }

    private async ValueTask EnqueueFlushAsync(CancellationToken cancellationToken)
    {
        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var item = new WalWorkItem { Kind = WalWorkKind.Flush, Completion = tcs };
        while (!_ring.TryEnqueue(in item))
            await Task.Yield();

        await tcs.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask EnqueueMaintenanceAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var begin = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var beginItem = new WalWorkItem { Kind = WalWorkKind.MaintenanceBegin, Completion = begin };
        while (!_ring.TryEnqueue(in beginItem))
            await Task.Yield();

        await begin.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
        await action(cancellationToken).ConfigureAwait(false);

        var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var resetSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var resetSequence = DetermineNextSequence(manifest, _opt);

        var end = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var endItem = new WalWorkItem
        {
            Kind = WalWorkKind.MaintenanceEnd,
            Completion = end,
            ResetSegmentIndex = resetSegmentIndex,
            ResetSequence = resetSequence,
        };
        while (!_ring.TryEnqueue(in endItem))
            await Task.Yield();

        await end.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private void EnqueueShutdown()
    {
        var shutdownItem = new WalWorkItem { Kind = WalWorkKind.Shutdown };
        while (!_ring.TryEnqueue(in shutdownItem))
            Thread.SpinWait(32);
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
            Span<byte> header = stackalloc byte[WalBinaryFraming.FileHeaderSize];
            WalBinaryFraming.WriteFileHeader(header);
            _segmentWriter.Write(header, 0);
        }

        _activeSegmentWrittenBytes = _segmentWriter.Length;
    }

    private void MaybeRollSegment(int incomingFrameBytes)
    {
        if (!_policy.ShouldRollSegment(_activeSegmentWrittenBytes, incomingFrameBytes))
            return;

        RollSegmentOnWalThread();
    }

    private void MaybeTimeOrSizeFlush()
    {
        var now = Environment.TickCount64;
        if (_batchBytesSinceFlush < BatchFlushBytes && now - _lastFlushTimestampMs < BatchFlushIntervalMs)
            return;

        FsyncOnWalThread();
    }

    private void FsyncOnWalThread()
    {
        if (!_dirty)
            return;

        _segmentWriter.Fsync();
        _dirty = false;
        _batchBytesSinceFlush = 0;
        _lastFlushTimestampMs = Environment.TickCount64;
    }

    private void ProcessAppend(WalWorkItem item)
    {
        EnsureSegmentOpen();
        MaybeRollSegment(item.FrameLength);
        var offset = _activeSegmentWrittenBytes;
        _segmentWriter.Write(item.FrameBytes!.AsSpan(0, item.FrameLength), offset);
        Volatile.Write(ref _activeSegmentWrittenBytes, offset + item.FrameLength);
        _batchBytesSinceFlush += item.FrameLength;
        _dirty = true;
        MaybeTimeOrSizeFlush();
    }

    private void RollSegmentOnWalThread()
    {
        FsyncOnWalThread();
        CurrentSegmentIndex++;
        _activeSegmentPath = JournalReadPath.BuildSegmentPath(_opt.DataDir, CurrentSegmentIndex);
        _segmentWriter.OpenSegment(_activeSegmentPath, append: false);
        Span<byte> header = stackalloc byte[WalBinaryFraming.FileHeaderSize];
        WalBinaryFraming.WriteFileHeader(header);
        _segmentWriter.Write(header, 0);
        Volatile.Write(ref _activeSegmentWrittenBytes, WalBinaryFraming.FileHeaderSize);
        _dirty = false;
        _batchBytesSinceFlush = 0;
    }

    private void ThrowIfWalThreadFailed()
    {
        if (Volatile.Read(ref _walThreadFailure) is { } failure)
            throw new InvalidOperationException("WAL thread failed.", failure);
    }

    private void WalThreadMain()
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
                    case WalWorkKind.Append:
                        ProcessAppend(item);
                        break;

                    case WalWorkKind.Flush:
                        FsyncOnWalThread();
                        CompleteWalWorkItem(item);
                        break;

                    case WalWorkKind.Shutdown:
                        FsyncOnWalThread();
                        return;

                    case WalWorkKind.MaintenanceBegin:
                        FsyncOnWalThread();
                        _activeSegmentPath = null;
                        CompleteWalWorkItem(item);
                        break;

                    case WalWorkKind.MaintenanceEnd:
                        CurrentSegmentIndex = item.ResetSegmentIndex;
                        lock (_sequenceLock)
                            _nextSequence = item.ResetSequence;
                        _activeSegmentWrittenBytes = 0;
                        _dirty = false;
                        CompleteWalWorkItem(item);
                        break;

                    default:
                        throw new InvalidOperationException($"unknown WAL work kind {item.Kind}.");
                }
            }
        }
        catch (Exception ex) when (ex is IOException or ObjectDisposedException or InvalidOperationException)
        {
            Volatile.Write(ref _walThreadFailure, ex);
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // WAL thread exits when background cancellation is requested during dispose.
        }
    }

    private bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
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
