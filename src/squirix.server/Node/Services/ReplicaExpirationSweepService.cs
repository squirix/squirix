using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Removes the expired entries of the replica groups this node leads that no read touches, through committed tombstones.</summary>
/// <remarks>
/// Storage keeps an entry past its deadline until a committed record removes it, so an expired key nobody reads would stay in memory for
/// good. Every pass reads the keys of the led groups whose deadline passed on the leader clock from the deadline index of the cache, so it
/// costs in proportion to the expired keys, not to the stored ones, and expires each through the committer of its group, at most
/// <see cref="MaxPerPass" /> per pass in all. Only a group this node may write to is
/// swept: one it leads statically, or one it leads by election with local authority. The committer decides each expiry again under its
/// commit gate, so a key written or touched since the walk is left alone. A group whose tombstone fails is logged once and skipped for the
/// rest of the pass, so it holds back no other group; the next pass retries it.
/// Activated hosts only: on other hosts the local clock decides expiry and <see cref="LocalExpirationSweepService" /> sweeps.
/// </remarks>
internal sealed class ReplicaExpirationSweepService : BackgroundService
{
    /// <summary>The time between two passes.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly ReplicaGroupCommitters _committers;
    private readonly ILocalCacheExpiredKeys _expiredKeys;
    private readonly INodeLocator _locator;
    private readonly ILogger<ReplicaExpirationSweepService> _log;

    /// <summary>Initializes a new instance of the <see cref="ReplicaExpirationSweepService" /> class.</summary>
    /// <param name="committers">The committers of the led groups; their clock decides expiry and times the passes.</param>
    /// <param name="expiredKeys">The stored keys past their deadline.</param>
    /// <param name="locator">The key owner lookup, which names the group of a key.</param>
    /// <param name="log">Logger of the failed passes.</param>
    internal ReplicaExpirationSweepService(
        ReplicaGroupCommitters committers,
        ILocalCacheExpiredKeys expiredKeys,
        INodeLocator locator,
        ILogger<ReplicaExpirationSweepService> log)
    {
        ArgumentNullException.ThrowIfNull(committers);
        ArgumentNullException.ThrowIfNull(expiredKeys);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(log);
        _committers = committers;
        _expiredKeys = expiredKeys;
        _locator = locator;
        _log = log;
        MaxPerPass = 1024;
    }

    /// <summary>Initializes the most keys one pass expires; 1024 unless set.</summary>
    internal int MaxPerPass
    {
        private get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);

            field = value;
        }
    }

    /// <summary>Runs one pass: expires the keys of the led groups whose deadline passed, at most <see cref="MaxPerPass" />, skipping a group once it failed.</summary>
    /// <param name="cancellationToken">Cancellation token of the host.</param>
    /// <returns>The number of keys expired or absent when the pass ended; a key found live again is not counted.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    /// <remarks>The keys of a group go to its committer in chunks of the batch limit, each chunk awaited before the next one.</remarks>
    internal async Task<int> SweepOnceAsync(CancellationToken cancellationToken) => (await SweepPassAsync(cancellationToken).ConfigureAwait(false)).Expired;

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _committers.Clock);
        await using var stop = timer.DisposeOnCancel(stoppingToken).ConfigureAwait(false);
        try
        {
            // A pass that hit its limit left expired keys behind: run the next one at once, until a pass ends short, appends nothing (so
            // it cannot have made progress), or the host stops.
            while (await timer.WaitForNextTickAsync(CancellationToken.None).ConfigureAwait(false))
            {
                (int Expired, bool Appended) pass;
                do
                {
                    pass = await SweepPassAsync(stoppingToken).ConfigureAwait(false);
                    stoppingToken.ThrowIfCancellationRequested();
                }
                while (pass.Appended && pass.Expired >= MaxPerPass);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            // A pass in flight observes the token while it commits; the wait itself ends without an exception.
        }

        if (stoppingToken.IsCancellationRequested)
            ServerLog.ReplicaExpirationSweepStopped(_log);
    }

    /// <summary>Expires a chunk of keys of one group together and waits for all of them.</summary>
    /// <param name="committer">The committer of the group.</param>
    /// <param name="keys">The keys of the group.</param>
    /// <param name="start">The first key of the chunk.</param>
    /// <param name="count">The number of keys of the chunk.</param>
    /// <param name="cancellationToken">Cancellation token of the host.</param>
    /// <returns>The number of keys expired or absent, and the first failure of the chunk, if any.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    private static async Task<(int Expired, Exception? Failure)> ExpireChunkAsync(ReplicaGroupCommitter committer, List<CacheKey> keys, int start, int count, CancellationToken cancellationToken)
    {
        var chunk = new (string CacheName, string Key)[count];
        for (var i = 0; i < count; i++)
            chunk[i] = (keys[start + i].Namespace, keys[start + i].Key);

        Task<NodeCacheEntry<object?>?>[] pending;
        try
        {
            pending = committer.ExpireMany(chunk, cancellationToken);
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested)
        {
            // A committer disposed under the pass throws before it returns any wait: the chunk failed as a whole.
            return (0, error);
        }

        var expired = 0;
        Exception? failure = null;
        foreach (var task in pending)
        {
            try
            {
                expired += await task.ConfigureAwait(false) == null ? 1 : 0;
            }
            catch (Exception error) when (!cancellationToken.IsCancellationRequested)
            {
                failure ??= error;
            }
        }

        return (expired, failure);
    }

    /// <summary>Runs one pass and tells whether it appended any tombstone.</summary>
    /// <param name="cancellationToken">Cancellation token of the host.</param>
    /// <returns>The number of keys expired or absent, and whether a committer of the pass appended a record.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    private async Task<(int Expired, bool Appended)> SweepPassAsync(CancellationToken cancellationToken)
    {
        var expired = 0;
        var appended = false;
        var now = _committers.Clock.GetUtcNow().UtcDateTime;

        // A key of a group this node only follows, or leads without authority, is expired by the leader of its group, never here. The
        // committer of each group is looked up once per pass.
        var committers = new Dictionary<string, ReplicaGroupCommitter?>(StringComparer.Ordinal);

        // The filter records the group it resolved for each key it accepts, so a ring change after the listing cannot send a key to
        // a group whose committer was never looked up.
        var ownerOfKey = new Dictionary<CacheKey, string>();
        var keys = _expiredKeys.ListExpired(
            now,
            MaxPerPass,
            key =>
            {
                var (owner, committer) = CommitterOf(committers, key);
                if (committer == null)
                    return false;

                ownerOfKey[key] = owner;
                return true;
            });
        var byOwner = new Dictionary<string, List<CacheKey>>(StringComparer.Ordinal);
        foreach (var key in keys)
        {
            if (!ownerOfKey.TryGetValue(key, out var owner))
                continue;

            if (!byOwner.TryGetValue(owner, out var ofOwner))
                byOwner[owner] = ofOwner = [];

            ofOwner.Add(key);
        }

        foreach (var (owner, ofOwner) in byOwner)
        {
            var committer = committers[owner]!;
            var appendedBefore = committer.LocalAppends.Snapshot().Appended;
            for (var start = 0; start < ofOwner.Count; start += ReplicaGroupCommitterCommits.MaxExpiriesPerCommit)
            {
                cancellationToken.ThrowIfCancellationRequested();
                var count = Math.Min(ReplicaGroupCommitterCommits.MaxExpiriesPerCommit, ofOwner.Count - start);
                var (done, failure) = await ExpireChunkAsync(committer, ofOwner, start, count, cancellationToken).ConfigureAwait(false);
                expired += done;
                if (failure == null)
                    continue;

                ServerLog.ReplicaExpirationSweepFailed(_log, owner, expired, failure);
                break;
            }

            appended |= committer.LocalAppends.Snapshot().Appended != appendedBefore;
        }

        return (expired, appended);
    }

    /// <summary>Finds the committer that may expire a key, once per group and pass.</summary>
    /// <param name="committers">The committers found in this pass, by group; <see langword="null" /> for a group this node may not write to.</param>
    /// <param name="key">The key.</param>
    /// <returns>The group of the key and its committer, which is <see langword="null" /> when this node may not write to the group.</returns>
    /// <remarks>Runs under the cache lock: it reads the ring and the published committers, and never calls into the cache.</remarks>
    private (string Owner, ReplicaGroupCommitter? Committer) CommitterOf(Dictionary<string, ReplicaGroupCommitter?> committers, CacheKey key)
    {
        var owner = _locator.GetOwner(key.Namespace, key.Key);
        if (!committers.TryGetValue(owner, out var committer))
            committers[owner] = committer = _committers.FindAuthorized(owner);

        return (owner, committer);
    }
}
