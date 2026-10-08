using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Removes the expired entries of the replica groups this node leads that no read touches, through committed tombstones.</summary>
/// <remarks>
/// Storage keeps an entry past its deadline until a committed record removes it, so an expired key nobody reads would stay in memory for
/// good. Every pass walks the stored entries once, picks the keys of the led groups whose deadline passed on the leader clock, and expires
/// each through the committer of its group, at most <see cref="MaxPerPass" /> per pass in all. Only a group this node may write to is
/// swept: one it leads statically, or one it leads by election with local authority. The committer decides each expiry again under its
/// commit gate, so a key written or touched since the walk is left alone. A group whose tombstone fails is logged once and skipped for the
/// rest of the pass, so it holds back no other group; the next pass retries it.
/// Activated hosts only: on other hosts the local clock decides expiry and nothing needs sweeping.
/// </remarks>
internal sealed class ReplicaExpirationSweepService : BackgroundService
{
    /// <summary>The time between two passes.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly ReplicaGroupCommitters _committers;
    private readonly INodeLocator _locator;
    private readonly ILogger<ReplicaExpirationSweepService> _log;
    private readonly ILocalCacheSnapshotReader<object?> _reader;

    /// <summary>Initializes a new instance of the <see cref="ReplicaExpirationSweepService" /> class.</summary>
    /// <param name="committers">The committers of the led groups; their clock decides expiry and times the passes.</param>
    /// <param name="reader">The stored entries.</param>
    /// <param name="locator">The key owner lookup, which names the group of a key.</param>
    /// <param name="log">Logger of the failed passes.</param>
    internal ReplicaExpirationSweepService(
        ReplicaGroupCommitters committers,
        ILocalCacheSnapshotReader<object?> reader,
        INodeLocator locator,
        ILogger<ReplicaExpirationSweepService> log)
    {
        ArgumentNullException.ThrowIfNull(committers);
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(log);
        _committers = committers;
        _reader = reader;
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
        HashSet<string>? failed = null;
        await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
        {
            if (expired == MaxPerPass)
                break;

            if (entry.ExpiresUtc is not { } deadline || deadline.Ticks > now.Ticks)
                continue;

            // A key of a group this node only follows, or leads without authority, is expired by the leader of its group, never here.
            var owner = _locator.GetOwner(key.Namespace, key.Key);
            if (failed?.Contains(owner) == true || _committers.FindAuthorized(owner) is not { } committer)
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
}
