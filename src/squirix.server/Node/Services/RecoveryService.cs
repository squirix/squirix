using System;
using System.Globalization;
using System.IO;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.JournalProto;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Replays the journal into the local in-memory cache on startup.
/// Skips expired entries so they are not resurrected after restart.
/// Restores exact CLR value types using the discriminated JSON format.
/// </summary>
/// <typeparam name="T">
/// The value type stored in the cache (e.g., <c>object?</c> for untyped payloads or a concrete DTO type).
/// </typeparam>
internal sealed class RecoveryService<T> : IHostedService
{
    private readonly IHostApplicationLifetime? _applicationLifetime;
    private readonly IdempotencyStore _idempotency;
    private readonly JournalStartupGate _journalStartupGate;
    private readonly ILocalCacheRecovery<T> _localCache;
    private readonly ILogger<RecoveryService<T>> _log;
    private readonly ManifestStore _manifestStore;
    private readonly PersistenceOptions _opt;
    private readonly RecoveryOptions _options;
    private Task? _replayTask;

    public RecoveryService(PersistenceOptions opt, ManifestStore manifestStore, ILocalCacheRecovery<T> localCache, RecoveryOptions options, ILogger<RecoveryService<T>> log)
        : this(opt, manifestStore, localCache, options, new JournalStartupGate(), new IdempotencyStore(), log)
    {
    }

    public RecoveryService(
        PersistenceOptions opt,
        ManifestStore manifestStore,
        ILocalCacheRecovery<T> localCache,
        RecoveryOptions options,
        JournalStartupGate journalStartupGate,
        ILogger<RecoveryService<T>> log)
        : this(opt, manifestStore, localCache, options, journalStartupGate, new IdempotencyStore(), log)
    {
    }

    public RecoveryService(
        PersistenceOptions opt,
        ManifestStore manifestStore,
        ILocalCacheRecovery<T> localCache,
        RecoveryOptions options,
        JournalStartupGate journalStartupGate,
        IdempotencyStore idempotency,
        ILogger<RecoveryService<T>> log,
        IHostApplicationLifetime? applicationLifetime = null)
    {
        _opt = opt;
        _manifestStore = manifestStore;
        _localCache = localCache;
        _options = options;
        _journalStartupGate = journalStartupGate;
        _idempotency = idempotency;
        _log = log;
        _applicationLifetime = applicationLifetime;
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        _ = _manifestStore.ReadCurrentOrDefault();

        if (_options.BlockOnStart)
        {
            await ReplayAsync(cancellationToken).ConfigureAwait(false);
            return;
        }

        _replayTask = Task.Run(() => ReplayInBackgroundAsync(cancellationToken), cancellationToken);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (_replayTask is null)
            return;

#pragma warning disable VSTHRD003
        // The replay task is owned by this hosted service and is awaited during shutdown.
        await _replayTask.ConfigureAwait(false);
#pragma warning restore VSTHRD003
    }

    private static InvalidOperationException CreateJournalDecodeFailure(ulong sequence, string operation, string key) => new(
        $"journal replay failed: undecodable entry payload at sequence {sequence} for operation '{operation}' on key '{key}'.");

    private static InvalidDataException CreateJournalReplayBoundaryFailure(int manifestCurrentJournal, int firstAvailableSegment, int lastAvailableSegment, bool snapshotPresent) =>
        new(
            string.Create(
                CultureInfo.InvariantCulture,
                $"journal recovery cannot determine a valid replay start. manifestCurrentJournal={manifestCurrentJournal}, firstAvailableJournal={(firstAvailableSegment > 0 ? firstAvailableSegment : 0)}, lastAvailableJournal={(lastAvailableSegment > 0 ? lastAvailableSegment : 0)}, chosenReplayStartSegment=0, snapshotPresent={snapshotPresent}."));

    private static int DetermineJournalOnlyReplayStart(Manifest manifest, int firstAvailableSegment, int lastAvailableSegment)
    {
        var manifestCurrentJournal = NormalizeSegmentIndex(manifest.CurrentJournal);
        var missingInitialSegment = firstAvailableSegment == 0 && manifestCurrentJournal != 1;
        var journalGapDetected = firstAvailableSegment > 0 && lastAvailableSegment < manifestCurrentJournal;
        return missingInitialSegment || journalGapDetected ? throw CreateJournalReplayBoundaryFailure(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment, false)
            : Math.Max(firstAvailableSegment, manifestCurrentJournal);
    }

    private static string FingerprintKey(CacheKey key) => key.ToString();

    private static bool IsExpiredForRecovery(CacheEntry<T> entry) =>
        (entry.ExpiresUtc is { } utc && utc <= DateTime.UtcNow) || (entry.Expiration is { } expiration && expiration <= TimeSpan.Zero);

    private static int NormalizeSegmentIndex(int segmentIndex) => segmentIndex > 0 ? segmentIndex : 1;

    private async Task ApplyJournalEnvelopeAsync(JournalEnvelope env, CancellationToken cancellationToken)
    {
        switch (env.OpCase)
        {
            case JournalEnvelope.OpOneofCase.Put:
            {
                var put = env.Put ?? throw new InvalidOperationException("journal envelope op case is Put but payload is missing.");
                var cacheNamespace = PersistedCacheNamespace.Normalize(put.Item.Namespace);
                var key = new CacheKey(cacheNamespace, put.Item.Key);

                if (!DiscriminatedEntryJsonReader.TryUtf8ToEntry<T>(put.Item.EntryJson.Memory, out var entry))
                    throw CreateJournalDecodeFailure(env.Seq, "put", key.Key);

                if (IsExpiredForRecovery(entry))
                    break;

                await _localCache.InsertForDurableRecoveryAsync(key, entry, cancellationToken).ConfigureAwait(false);
                _idempotency.RestoreInsert(put.OperationId, IdempotencyStore.BuildInsertFingerprint(FingerprintKey(key), put.Item.EntryJson.Span));
                break;
            }

            case JournalEnvelope.OpOneofCase.Remove:
            {
                var remove = env.Remove ?? throw new InvalidOperationException("journal envelope op case is Remove but payload is missing.");
                var cacheNamespace = PersistedCacheNamespace.Normalize(remove.Namespace);
                _ = await _localCache.RemoveForDurableRecoveryAsync(new CacheKey(cacheNamespace, remove.Key), cancellationToken).ConfigureAwait(false);
                break;
            }

            case JournalEnvelope.OpOneofCase.RemoveExpiration:
            {
                var removeExpiration = env.RemoveExpiration ?? throw new InvalidOperationException("journal envelope op case is RemoveExpiration but payload is missing.");
                var cacheNamespace = PersistedCacheNamespace.Normalize(removeExpiration.Namespace);
                _ = await _localCache.RemoveExpirationForDurableRecoveryAsync(new CacheKey(cacheNamespace, removeExpiration.Key), cancellationToken).ConfigureAwait(false);
                break;
            }

            case JournalEnvelope.OpOneofCase.TouchExpiration:
            {
                var touchExpiration = env.TouchExpiration ?? throw new InvalidOperationException("journal envelope op case is TouchExpiration but payload is missing.");
                var cacheNamespace = PersistedCacheNamespace.Normalize(touchExpiration.Namespace);
                var expiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(touchExpiration.ExpiresUnixMs).UtcDateTime;
                _ = await _localCache.TouchExpirationForDurableRecoveryAsync(new CacheKey(cacheNamespace, touchExpiration.Key), expiresUtc, cancellationToken)
                                     .ConfigureAwait(false);
                break;
            }

            case JournalEnvelope.OpOneofCase.None:
                break;

            default:
                throw new ArgumentOutOfRangeException(nameof(env), env.OpCase, "Unsupported journal op.");
        }
    }

    private async Task ApplySnapshotEntriesAsync(SnapshotLoadResult<T> snapshot, CancellationToken cancellationToken)
    {
        foreach (var (k, entry) in snapshot.Entries)
            await _localCache.InsertForDurableRecoveryAsync(k, entry, cancellationToken).ConfigureAwait(false);
    }

    private (int FirstAvailableSegment, int LastAvailableSegment) GetJournalSegmentRange()
    {
        var firstAvailableSegment = 0;
        var lastAvailableSegment = 0;
        foreach (var segment in JournalReader.EnumerateSegments(_opt.DataDir, 1))
        {
            if (firstAvailableSegment == 0)
                firstAvailableSegment = segment.Index;

            lastAvailableSegment = segment.Index;
        }

        return (firstAvailableSegment, lastAvailableSegment);
    }

    private void HandleSnapshotLoadFailure(ReplayContext context, string snapshotPath, out int fromSegment, out ulong lastAppliedSeq)
    {
        LogManager.RecoveryFailedToLoadSnapshot(_log, snapshotPath);
        RequireFullJournalReplayRange(context.ManifestCurrentJournal);
        fromSegment = context.FirstJournalSegmentOrDefault;
        lastAppliedSeq = 0;
    }

    private ReplayContext LoadReplayContext()
    {
        var manifest = _manifestStore.ReadCurrentOrDefault();
        var snapRef = manifest.LastSnapshot;
        var manifestCurrentJournal = NormalizeSegmentIndex(manifest.CurrentJournal);
        var (firstAvailableSegment, lastAvailableSegment) = GetJournalSegmentRange();
        var firstJournalSegmentOrDefault = firstAvailableSegment > 0 ? firstAvailableSegment : 1;
        var lastAppliedSeq = snapRef?.LastAppliedSequence ?? 0UL;
        var fromSegment = snapRef is null ? DetermineJournalOnlyReplayStart(manifest, firstAvailableSegment, lastAvailableSegment) : firstJournalSegmentOrDefault;

        return new ReplayContext(snapRef, manifestCurrentJournal, firstAvailableSegment, firstJournalSegmentOrDefault, fromSegment, lastAppliedSeq);
    }

    private void LogReplayBoundary(ReplayContext context, int fromSegment) => LogManager.RecoveryReplayBoundary(
        _log,
        context.SnapshotReference is not null,
        context.ManifestCurrentJournal,
        context.FirstAvailableSegment,
        fromSegment);

    private async Task ReplayAsync(CancellationToken cancellationToken)
    {
        try
        {
            var context = LoadReplayContext();
            var replayState = await RestoreSnapshotIfPresentAsync(context, cancellationToken).ConfigureAwait(false);
            LogReplayBoundary(context, replayState.FromSegment);
            await ReplayJournalSegmentsAsync(replayState.FromSegment, replayState.LastAppliedSequence, cancellationToken).ConfigureAwait(false);

            LogManager.RecoveryComplete(_log, replayState.FromSegment, replayState.LastAppliedSequence);
            _journalStartupGate.Open();
        }
        catch (IOException)
        {
            LogManager.JournalRecoveryFailed(_log);
            throw;
        }
        catch (UnauthorizedAccessException)
        {
            LogManager.JournalRecoveryFailed(_log);
            throw;
        }
        catch (InvalidDataException)
        {
            LogManager.JournalRecoveryFailed(_log);
            throw;
        }
        catch (InvalidOperationException)
        {
            LogManager.JournalRecoveryFailed(_log);
            throw;
        }
        catch (JsonException)
        {
            LogManager.JournalRecoveryFailed(_log);
            throw;
        }
    }

    private async Task ReplayInBackgroundAsync(CancellationToken cancellationToken)
    {
        try
        {
            await ReplayAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Host shutdown path.
        }
        catch (IOException)
        {
            // Non-blocking mode must not silently continue after failed recovery.
            _applicationLifetime?.StopApplication();
        }
        catch (UnauthorizedAccessException)
        {
            // Non-blocking mode must not silently continue after failed recovery.
            _applicationLifetime?.StopApplication();
        }
        catch (InvalidDataException)
        {
            // Non-blocking mode must not silently continue after failed recovery.
            _applicationLifetime?.StopApplication();
        }
        catch (InvalidOperationException)
        {
            // Non-blocking mode must not silently continue after failed recovery.
            _applicationLifetime?.StopApplication();
        }
        catch (JsonException)
        {
            // Non-blocking mode must not silently continue after failed recovery.
            _applicationLifetime?.StopApplication();
        }
    }

    private async Task ReplayJournalSegmentsAsync(int fromSegment, ulong lastAppliedSeq, CancellationToken cancellationToken)
    {
        foreach (var env in JournalReader.ReadAll(_opt.DataDir, fromSegment, cancellationToken))
        {
            if (env.Seq <= lastAppliedSeq)
                continue;

            await ApplyJournalEnvelopeAsync(env, cancellationToken).ConfigureAwait(false);
        }
    }

    private void RequireFullJournalReplayRange(int manifestCurrentJournal)
    {
        var (firstAvailableSegment, lastAvailableSegment) = GetJournalSegmentRange();
        if (firstAvailableSegment > 1 || (lastAvailableSegment > 0 && lastAvailableSegment < manifestCurrentJournal))
            throw CreateJournalReplayBoundaryFailure(manifestCurrentJournal, firstAvailableSegment, lastAvailableSegment, true);
    }

    private async Task<ReplayState> RestoreSnapshotIfPresentAsync(ReplayContext context, CancellationToken cancellationToken)
    {
        var fromSegment = context.FromSegment;
        var lastAppliedSeq = context.LastAppliedSequence;
        var snapshotReference = context.SnapshotReference;

        if (snapshotReference != null && !string.IsNullOrWhiteSpace(snapshotReference.Path) && File.Exists(snapshotReference.Path))
        {
            SnapshotLoadResult<T>? snapshot = null;
            try
            {
                snapshot = await SnapshotReader.LoadStrictAsync<T>(snapshotReference.Path, cancellationToken: cancellationToken).ConfigureAwait(false);
            }
            catch (IOException)
            {
                HandleSnapshotLoadFailure(context, snapshotReference.Path, out fromSegment, out lastAppliedSeq);
            }
            catch (JsonException)
            {
                HandleSnapshotLoadFailure(context, snapshotReference.Path, out fromSegment, out lastAppliedSeq);
            }
            catch (InvalidDataException)
            {
                HandleSnapshotLoadFailure(context, snapshotReference.Path, out fromSegment, out lastAppliedSeq);
            }
            catch (InvalidOperationException)
            {
                HandleSnapshotLoadFailure(context, snapshotReference.Path, out fromSegment, out lastAppliedSeq);
            }
            catch (UnauthorizedAccessException)
            {
                HandleSnapshotLoadFailure(context, snapshotReference.Path, out fromSegment, out lastAppliedSeq);
            }

            if (snapshot is null)
                return new ReplayState(fromSegment, lastAppliedSeq);

            await ApplySnapshotEntriesAsync(snapshot, cancellationToken).ConfigureAwait(false);
            _idempotency.RestoreSnapshotRecords(snapshot.IdempotencyRecords);
            fromSegment = snapshotReference.ReplayFromJournalSegment > 0 ? snapshotReference.ReplayFromJournalSegment : 1;
            LogManager.RecoveryLoadedSnapshot(_log, snapshotReference.Index, lastAppliedSeq);
            return new ReplayState(fromSegment, lastAppliedSeq);
        }

        if (snapshotReference == null)
            return new ReplayState(fromSegment, lastAppliedSeq);

        LogManager.RecoveryFailedToLoadSnapshot(_log, snapshotReference.Path ?? "<null>");
        RequireFullJournalReplayRange(context.ManifestCurrentJournal);
        fromSegment = context.FirstJournalSegmentOrDefault;
        lastAppliedSeq = 0;
        return new ReplayState(fromSegment, lastAppliedSeq);
    }

    private sealed record ReplayContext(
        Manifest.SnapshotRef? SnapshotReference,
        int ManifestCurrentJournal,
        int FirstAvailableSegment,
        int FirstJournalSegmentOrDefault,
        int FromSegment,
        ulong LastAppliedSequence);

    private sealed record ReplayState(int FromSegment, ulong LastAppliedSequence);
}
