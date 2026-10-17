using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
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

    /// <summary>Gets the most keys one pass expires; 1024 unless set.</summary>
    internal int MaxPerPass
    {
        get;
        init
        {
            ArgumentOutOfRangeException.ThrowIfNegativeOrZero(value);

            field = value;
        }
    }

    /// <summary>Runs one pass: expires the keys of the led groups whose deadline passed, at most <see cref="MaxPerPass" />, skipping a group once it failed.</summary>
    /// <param name="cancellationToken">Cancellation token of the host.</param>
    /// <returns>The number of keys expired, or found live again, before the pass ended.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<int> SweepOnceAsync(CancellationToken cancellationToken)
    {
        var expired = 0;
        var now = _committers.Clock.GetUtcNow().UtcDateTime;

        // A key of a group this node only follows, or leads without authority, is expired by the leader of its group, never here. The
        // committer of each group is looked up once per pass.
        var committers = new Dictionary<string, ReplicaGroupCommitter?>(StringComparer.Ordinal);
        var keys = _expiredKeys.ListExpired(now, MaxPerPass, key => CommitterOf(committers, key) != null);
        HashSet<string>? failed = null;
        foreach (var key in keys)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var owner = _locator.GetOwner(key.Namespace, key.Key);
            if (failed?.Contains(owner) == true || committers[owner] is not { } committer)
                continue;

            try
            {
                _ = await committer.ExpireAsync(key.Namespace, key.Key, cancellationToken).ConfigureAwait(false);
                expired++;
            }
            catch (Exception error) when (!cancellationToken.IsCancellationRequested)
            {
                ServerLog.ReplicaExpirationSweepFailed(_log, owner, expired, error);
                failed ??= [with(StringComparer.Ordinal)];
                _ = failed.Add(owner);
            }
        }

        return expired;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _committers.Clock);
        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
                _ = await SweepOnceAsync(stoppingToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
            ServerLog.ReplicaExpirationSweepStopped(_log);
        }
    }

    /// <summary>Finds the committer that may expire a key, once per group and pass.</summary>
    /// <param name="committers">The committers found in this pass, by group; <see langword="null" /> for a group this node may not write to.</param>
    /// <param name="key">The key.</param>
    /// <returns>The committer of the group of the key, or <see langword="null" /> when this node may not write to it.</returns>
    /// <remarks>Runs under the cache lock: it reads the ring and the published committers, and never calls into the cache.</remarks>
    private ReplicaGroupCommitter? CommitterOf(Dictionary<string, ReplicaGroupCommitter?> committers, CacheKey key)
    {
        var owner = _locator.GetOwner(key.Namespace, key.Key);
        if (!committers.TryGetValue(owner, out var committer))
            committers[owner] = committer = _committers.FindAuthorized(owner);

        return committer;
    }
}
