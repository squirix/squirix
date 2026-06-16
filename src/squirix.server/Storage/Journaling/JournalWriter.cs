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
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Storage.Journaling.Json;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

internal sealed class JournalWriter : IJournalCoordinator
{
    private readonly CancellationTokenSource _bgCts = new();
    private readonly Task _flushLoopTask;
    private readonly PeriodicTimer _flushTimer;
    private readonly JournalDurabilityGroupCommit? _groupCommit;
    private readonly SemaphoreSlim _ioGate = new(1, 1);
    private readonly ManifestStore _manifestStore;
    private readonly SemaphoreSlim _mutationGate = new(1, 1);
    private readonly PersistenceOptions _opt;
    private readonly Lock _pendingMemoryApplyLock = new();
    private readonly Lock _sequenceLock = new();
    private readonly JournalStartupGate _startupGate;
    private readonly Lock _streamLock = new();
    private double _avgAppendLatencyMs;
    private long _bytes;
    private volatile bool _dirty;
    private int _disposed;
    private Exception? _flushLoopFailure;
    private ulong _nextSequence;
    private long _ops;
    private int _pendingMemoryApplyCount;
    private TaskCompletionSource? _pendingMemoryApplyDrained;
    private FileStream? _stream;

    private JournalWriter(PersistenceOptions opt, Manifest manifest, ManifestStore manifestStore, JournalStartupGate startupGate)
    {
        ArgumentNullException.ThrowIfNull(startupGate);
        _opt = opt;
        _manifestStore = manifestStore;
        _startupGate = startupGate;
        _groupCommit = _opt.IsJournalGroupCommitEnabled ? new JournalDurabilityGroupCommit(FlushAsync, _opt) : null;
        _ = DirectoryEx.CreateDirectory(_opt.DataDir);
        CurrentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        NextSequence = DetermineNextSequence(manifest, _opt);
        _flushTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(Math.Max(1, _opt.FlushIntervalMs)));
        _flushLoopTask = FlushLoopAsync(_bgCts.Token);
    }

    public event EventHandler? OnAppended;

    public long AppendedBytes => Interlocked.Read(ref _bytes);

    public long AppendedOps => Interlocked.Read(ref _ops);

    public int CurrentSegmentIndex { get; private set; }

    public bool HasFlushLoopFailure => Volatile.Read(ref _flushLoopFailure) is not null;

    public bool IsJournalGroupCommitEnabled => _opt.IsJournalGroupCommitEnabled;

    public ulong NextSequence
    {
        get
        {
            lock (_sequenceLock)
                return _nextSequence;
        }

        private set
        {
            lock (_sequenceLock)
                _nextSequence = value;
        }
    }

    public double RecentAppendLatencyMs => Volatile.Read(ref _avgAppendLatencyMs);

    /// <summary>Gets the bytes written to the active journal segment file for the current roll window. For unit tests only.</summary>
    internal long ActiveSegmentWrittenBytes { get; private set; }

    /// <summary>
    /// Gets a value indicating whether appended journal bytes are not yet covered by <see cref="FlushCoreAsync" /> (including strict fsync).
    /// </summary>
    internal bool IsDurabilityFlushPending => _dirty;

    public static async Task<JournalWriter> CreateAsync(
        PersistenceOptions opt,
        Manifest manifest,
        ManifestStore manifestStore,
        JournalStartupGate startupGate,
        CancellationToken cancellationToken = default)
    {
        await PrepareActiveSegmentForSequenceScanAsync(manifest, opt, cancellationToken).ConfigureAwait(false);
        return new JournalWriter(opt, manifest, manifestStore, startupGate);
    }

    public async ValueTask FlushAsync(CancellationToken cancellationToken)
    {
        ThrowIfFlushLoopFailed();

        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FlushCoreAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    public ValueTask AppendPutAsync(CacheKey key, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        EntryPayloadSizeGuard.EnsureDiscriminatedJsonWithinLimit(discriminatedEntryJson);
        return AppendPutAsync(key.Key, key.Namespace, discriminatedEntryJson, operationId, cancellationToken);
    }

    public ValueTask AppendRemoveAsync(CacheKey key, CancellationToken cancellationToken) => AppendAsync(
        new JournalEnvelope
        {
            Seq = AllocateSequence(),
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Remove = new Remove { Key = key.Key, Namespace = key.Namespace },
        },
        cancellationToken);

    public ValueTask AppendRemoveExpirationAsync(CacheKey key, CancellationToken cancellationToken) => AppendAsync(
        new JournalEnvelope
        {
            Seq = AllocateSequence(),
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            RemoveExpiration = new RemoveExpiration { Key = key.Key, Namespace = key.Namespace },
        },
        cancellationToken);

    public ValueTask AppendTouchExpirationAsync(CacheKey key, DateTime expiresUtc, CancellationToken cancellationToken) => AppendAsync(
        new JournalEnvelope
        {
            Seq = AllocateSequence(),
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            TouchExpiration = new TouchExpiration
            {
                Key = key.Key,
                Namespace = key.Namespace,
                ExpiresUnixMs = new DateTimeOffset(DateTime.SpecifyKind(expiresUtc, DateTimeKind.Utc)).ToUnixTimeMilliseconds(),
            },
        },
        cancellationToken);

    /// <summary>Waits until appended journal bytes are durable. Uses group commit when configured; otherwise flushes immediately.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A task that completes when durability is established for prior appends.</returns>
    public ValueTask AwaitDurabilityCommitAsync(CancellationToken cancellationToken)
    {
        ThrowIfFlushLoopFailed();
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
            _flushTimer.Dispose();
        }
        catch (ObjectDisposedException)
        {
            // Concurrent teardown can dispose the CTS or timer before this owner observes it.
        }

        if (_groupCommit is not null)
            await _groupCommit.CancelPendingAsync(new ObjectDisposedException(nameof(JournalWriter))).ConfigureAwait(false);

        await AwaitFlushLoopDuringDisposeAsync(failures).ConfigureAwait(false);
        await FlushDuringDisposeAsync(failures).ConfigureAwait(false);
        await DisposeStreamDuringDisposeAsync(failures).ConfigureAwait(false);

        _bgCts.Dispose();
        _ioGate.Dispose();
        _mutationGate.Dispose();

        ThrowDisposeFailures(failures);
    }

    public async ValueTask ExecuteMaintenanceExclusiveAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);
        ThrowIfFlushLoopFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await ExecuteMaintenanceCoreAsync(action, cancellationToken).ConfigureAwait(false);
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
        ThrowIfFlushLoopFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await WaitForSnapshotCutAdmissionAsync(cancellationToken).ConfigureAwait(false);
        ulong seqAtFlush;
        TBarrier barrierState;
        try
        {
            await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            try
            {
                await FlushCoreAsync(cancellationToken).ConfigureAwait(false);
                seqAtFlush = NextSequence > 0 ? NextSequence - 1UL : 0UL;
            }
            finally
            {
                _ = _ioGate.Release();
            }

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
        ThrowIfFlushLoopFailed();

        try
        {
            await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
            await _mutationGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (ObjectDisposedException ex)
        {
            throw new InvalidOperationException("journal writer is disposed.", ex);
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

    private static long ComputeValidLength(FileStream stream)
    {
        if (stream.Length == 0)
            return 0;

        stream.Position = 0;
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        if (!StreamEx.TryReadExact(stream, header))
            throw JournalFraming.CreateTruncatedHeaderException(stream.Length);

        JournalFraming.ThrowIfSegmentHeaderInvalid(stream.Length, header);

        long validLength = JournalFraming.FileHeaderSize;
        while (true)
        {
            var frameOffset = validLength;
            var read = JournalFrameReader.ReadNext(stream, validLength, out var rentedBuffer, out _);
            if (read.Status is JournalFrameReadStatus.EndOfFile or not JournalFrameReadStatus.Success)
                return validLength;

            validLength = read.NextFrameOffset;
            if (rentedBuffer is null)
                throw new InvalidDataException($"journal segment missing payload buffer at offset {frameOffset.ToString(CultureInfo.InvariantCulture)}.");

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

        // Sequence discovery scans only the active journal prefix (manifest CurrentJournal onward, intersected
        // with on-disk segments). Segments below CurrentJournal are compaction/roll leftovers; replay and
        // manifest NextSequence already subsume their history, so reading them would add latency and
        // could resurrect stale sequence numbers inconsistent with the active log boundary.
        var manifestCurrentJournal = manifest.CurrentJournal > 0 ? manifest.CurrentJournal : 1;
        var firstAvailableSegment = 0;
        var lastAvailableSegment = 0;
        foreach (var segment in JournalReader.EnumerateSegments(options.DataDir, 1))
        {
            if (firstAvailableSegment is 0)
                firstAvailableSegment = segment.Index;

            lastAvailableSegment = segment.Index;
        }

        ThrowIfJournalOnlyTopologyDisjointForSequenceInit(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment);

        var scanStartSegment = firstAvailableSegment is 0 ? 1 : Math.Max(firstAvailableSegment, manifestCurrentJournal);

        foreach (var env in JournalReader.ReadAll(options.DataDir, scanStartSegment, CancellationToken.None))
        {
            if (env.Seq >= next)
                next = env.Seq + 1UL;
        }

        return next;
    }

    private static FileOptions GetJournalFileOptions()
    {
        var opts = FileOptions.Asynchronous;
        if (OperatingSystem.IsWindows())
            opts |= FileOptions.WriteThrough;
        return opts;
    }

    private static async Task PrepareActiveSegmentForSequenceScanAsync(Manifest manifest, PersistenceOptions options, CancellationToken cancellationToken)
    {
        var segmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
        var path = PathEx.Combine(
            options.DataDir,
            $"{StorageFilePrefixes.Journal}{segmentIndex.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
        if (!File.Exists(path))
            return;

        var stream = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, FileOptions.None);
        try
        {
            await RepairTornTailIfNeededAsync(stream, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            await stream.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static async Task RepairTornTailIfNeededAsync(FileStream stream, CancellationToken cancellationToken)
    {
        try
        {
            var validLength = ComputeValidLength(stream);
            if (validLength == stream.Length)
                return;

            stream.SetLength(validLength);
            if (validLength == 0)
                JournalFraming.WriteFileHeader(stream);

            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (InvalidDataException) when (stream.Length > 0)
        {
            stream.SetLength(0);
            JournalFraming.WriteFileHeader(stream);
            await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
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
                throw new AggregateException("journal writer disposal failed.", failures);
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

    private ulong AllocateSequence()
    {
        lock (_sequenceLock)
            return _nextSequence++;
    }

    private async ValueTask AppendAsync(JournalEnvelope env, CancellationToken cancellationToken)
    {
        ThrowIfFlushLoopFailed();

        await _startupGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var payload = RecordCodec.Serialize(env);
            var frameLen = JournalFraming.FrameHeaderSize + payload.Length + JournalFraming.FrameFooterSize;
            await EnsureSegmentCapacityForFrameAsync(frameLen, cancellationToken).ConfigureAwait(false);

            _ = await AppendFrameAsync(payload, cancellationToken).ConfigureAwait(false);
            ActiveSegmentWrittenBytes += frameLen;
            _dirty = true;

            // Publish buffered bytes to the OS so other handles (tail tools) observe a non-empty journal
            // without waiting for the periodic flush timer. Does not replace StrictFsync disk flush in FlushCoreAsync.
            var stream = _stream;
            if (stream is not null)
                await stream.FlushAsync(cancellationToken).ConfigureAwait(false);

            if (frameLen > Math.Max(1, _opt.JournalMaxSegmentMb) * 1024L * 1024L)
                await RollSegmentAsync(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    private async Task<int> AppendFrameAsync(byte[] payload, CancellationToken cancellationToken)
    {
        var stream = await GetOrCreateStreamAsync(cancellationToken).ConfigureAwait(false);
        var frameLen = JournalFraming.FrameHeaderSize + payload.Length + JournalFraming.FrameFooterSize;
        var frameStart = stream.Position;
        var sw = Stopwatch.StartNew();

        try
        {
            Span<byte> header = stackalloc byte[JournalFraming.FrameHeaderSize];
            BinaryPrimitives.WriteInt32LittleEndian(header, payload.Length);
            stream.Write(header);

            await stream.WriteAsync(payload, cancellationToken).ConfigureAwait(false);

            var crc = Crc32C.Compute(payload);
            Span<byte> footer = stackalloc byte[JournalFraming.FrameFooterSize];
            BinaryPrimitives.WriteUInt32LittleEndian(footer, crc);
            stream.Write(footer);
        }
        catch (IOException)
        {
            TruncateActiveSegmentAfterFailedFrame(stream, frameStart);
            throw;
        }
        catch (ObjectDisposedException)
        {
            TruncateActiveSegmentAfterFailedFrame(stream, frameStart);
            throw;
        }
        catch (OperationCanceledException)
        {
            TruncateActiveSegmentAfterFailedFrame(stream, frameStart);
            throw;
        }

        sw.Stop();
        var elapsedMs = sw.Elapsed.TotalMilliseconds;
        var currentLatency = Volatile.Read(ref _avgAppendLatencyMs);
        var nextLatency = currentLatency <= 0 ? elapsedMs : (currentLatency * 0.9) + (elapsedMs * 0.1);
        Volatile.Write(ref _avgAppendLatencyMs, nextLatency);

        _ = Interlocked.Add(ref _bytes, frameLen);
        _ = Interlocked.Increment(ref _ops);
        OnAppended?.Invoke(this, EventArgs.Empty);
        return frameLen;
    }

    private ValueTask AppendPutAsync(string key, string cacheNamespace, byte[] discriminatedEntryJson, string? operationId, CancellationToken cancellationToken)
    {
        var journalEnvelope = new JournalEnvelope
        {
            Seq = AllocateSequence(),
            UnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
            Put = new Put
            {
                Item = new EntryPair { Key = key, Namespace = cacheNamespace, EntryJson = ByteString.CopyFrom(discriminatedEntryJson) },
                OperationId = operationId ?? string.Empty,
            },
        };
        return AppendAsync(journalEnvelope, cancellationToken);
    }

    private async ValueTask AwaitFlushLoopDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
#pragma warning disable VSTHRD003

            // The flush loop task is owned by this writer and is awaited during disposal.
            await _flushLoopTask.ConfigureAwait(false);
#pragma warning restore VSTHRD003
        }
        catch (OperationCanceledException) when (_bgCts.IsCancellationRequested)
        {
            // Expected cooperative shutdown.
        }
        catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) is not 0)
        {
            // The timer may be disposed while the loop is waiting for the next tick.
        }
        catch (IOException ex)
        {
            failures.Add(ex);
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
        catch (InvalidOperationException ex)
        {
            failures.Add(ex);
        }
    }

    private async ValueTask DisposeStreamAsync()
    {
        FileStream? stream;
        lock (_streamLock)
        {
            stream = _stream;
            _stream = null;
        }

        if (stream is not null)
            await stream.DisposeAsync().ConfigureAwait(false);
    }

    private async ValueTask DisposeStreamDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
            await DisposeStreamAsync().ConfigureAwait(false);
        }
        catch (IOException ex)
        {
            failures.Add(ex);
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
        catch (InvalidOperationException ex)
        {
            failures.Add(ex);
        }
    }

    private async ValueTask EnsureSegmentCapacityForFrameAsync(int frameLen, CancellationToken cancellationToken)
    {
        var maxBytes = Math.Max(1, _opt.JournalMaxSegmentMb) * 1024L * 1024L;
        if (frameLen > maxBytes)
            return;

        if (ActiveSegmentWrittenBytes > 0 && ActiveSegmentWrittenBytes + frameLen > maxBytes)
            await RollSegmentAsync(cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask ExecuteMaintenanceCoreAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        await _ioGate.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            await FlushCoreAsync(cancellationToken).ConfigureAwait(false);
            await DisposeStreamAsync().ConfigureAwait(false);

            await action(cancellationToken).ConfigureAwait(false);

            var manifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
            CurrentSegmentIndex = manifest.CurrentJournal <= 0 ? 1 : manifest.CurrentJournal;
            NextSequence = DetermineNextSequence(manifest, _opt);
            ActiveSegmentWrittenBytes = 0;
            _dirty = false;
        }
        finally
        {
            _ = _ioGate.Release();
        }
    }

    private async ValueTask FlushCoreAsync(CancellationToken cancellationToken)
    {
        var stream = await GetOrCreateStreamAsync(cancellationToken).ConfigureAwait(false);
        await stream.FlushAsync(cancellationToken).ConfigureAwait(false);
        _dirty = false;
    }

    private async ValueTask FlushDuringDisposeAsync(List<Exception> failures)
    {
        try
        {
            await _ioGate.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                await FlushCoreAsync(CancellationToken.None).ConfigureAwait(false);
            }
            finally
            {
                _ = _ioGate.Release();
            }
        }
        catch (IOException ex)
        {
            failures.Add(ex);
        }
        catch (ObjectDisposedException ex)
        {
            failures.Add(ex);
        }
        catch (InvalidOperationException ex)
        {
            failures.Add(ex);
        }
    }

    private async Task FlushLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            while (await _flushTimer.WaitForNextTickAsync(cancellationToken).ConfigureAwait(false))
            {
                if (_dirty)
                    await FlushAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            // Periodic flush loop stops when the journal writer is shutting down and the flush timer wait is canceled.
        }
        catch (ObjectDisposedException) when (Volatile.Read(ref _disposed) is not 0)
        {
            // Flush loop may outlive the writer by one tick; dispose completes the timer and pump exits without surfacing as an error.
        }
        catch (IOException ex)
        {
            Volatile.Write(ref _flushLoopFailure, ex);
            throw;
        }
        catch (ObjectDisposedException ex)
        {
            Volatile.Write(ref _flushLoopFailure, ex);
            throw;
        }
        catch (InvalidOperationException ex)
        {
            Volatile.Write(ref _flushLoopFailure, ex);
            throw;
        }
    }

    private async Task<FileStream> GetOrCreateStreamAsync(CancellationToken cancellationToken)
    {
        var existing = _stream;
        if (existing is not null)
            return existing;

        var candidate = await OpenSegmentAsync(CurrentSegmentIndex, true, cancellationToken).ConfigureAwait(false);
        var adoptCandidate = false;

        lock (_streamLock)
        {
            existing = _stream;
            if (existing is null)
            {
                _stream = candidate;
                ActiveSegmentWrittenBytes = candidate.Length;
                adoptCandidate = true;
            }
        }

        if (adoptCandidate)
            return candidate;

        await candidate.DisposeAsync().ConfigureAwait(false);
        return existing!;
    }

    private bool HasPendingMemoryApply()
    {
        lock (_pendingMemoryApplyLock)
            return _pendingMemoryApplyCount > 0;
    }

    private async Task<FileStream> OpenSegmentAsync(int idx, bool append, CancellationToken cancellationToken)
    {
        var path = PathEx.Combine(_opt.DataDir, $"{StorageFilePrefixes.Journal}{idx.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
        var modes = append ? FileMode.OpenOrCreate : FileMode.Create;
        var fs = new FileStream(path, modes, FileAccess.ReadWrite, FileShare.Read | FileShare.Delete, 64 * 1024, GetJournalFileOptions());
        if (fs.Length == 0)
        {
            JournalFraming.WriteFileHeader(fs);
        }
        else if (append)
        {
            await RepairTornTailIfNeededAsync(fs, cancellationToken).ConfigureAwait(false);
            _ = fs.Seek(0, SeekOrigin.End);
        }

        return fs;
    }

    private async Task RollSegmentAsync(CancellationToken cancellationToken)
    {
        await FlushCoreAsync(cancellationToken).ConfigureAwait(false);
        var current = _stream ?? throw new InvalidOperationException("journal stream is not initialized.");
        await current.DisposeAsync().ConfigureAwait(false);
        CurrentSegmentIndex++;
        var nextStream = await OpenSegmentAsync(CurrentSegmentIndex, false, cancellationToken).ConfigureAwait(false);
        lock (_streamLock)
        {
            _stream = nextStream;
            ActiveSegmentWrittenBytes = nextStream.Length;
        }

        _dirty = false;
        var prevManifest = await _manifestStore.ReadCurrentOrDefaultAsync(cancellationToken).ConfigureAwait(false);
        var manifest = new Manifest
        {
            Format = prevManifest.Format is 0 ? 1 : prevManifest.Format,
            CurrentJournal = CurrentSegmentIndex,
            NextSequence = NextSequence,
            LastSnapshot = prevManifest.LastSnapshot,
        };
        await _manifestStore.WriteAsync(manifest, cancellationToken).ConfigureAwait(false);
    }

    private void ThrowIfFlushLoopFailed()
    {
        if (Volatile.Read(ref _flushLoopFailure) is not { } failure)
            return;

        throw new InvalidOperationException("journal periodic flush loop failed.", failure);
    }

    private void TruncateActiveSegmentAfterFailedFrame(FileStream stream, long frameStart)
    {
        if (stream.Position <= frameStart)
            return;

        stream.SetLength(frameStart);
        stream.Position = frameStart;
        ActiveSegmentWrittenBytes = frameStart;
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
