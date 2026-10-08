using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.LocalCache;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Removes the expired entries of the owned replica group that no read touches, through committed tombstones.</summary>
/// <remarks>
/// Storage keeps an entry past its deadline until a committed record removes it, so an expired key nobody reads would stay in memory for
/// good. Every pass walks the stored entries, picks the keys this node owns whose deadline passed on the leader clock, and expires them
/// through the committer, at most <see cref="MaxPerPass" /> per pass. The committer decides each expiry again under its commit gate, so a
/// key written or touched since the walk is left alone. A pass stops at its first failure and logs it once; the next pass retries.
/// Activated hosts only: on other hosts the local clock decides expiry and nothing needs sweeping.
/// </remarks>
internal sealed class ReplicaExpirationSweepService : BackgroundService
{
    /// <summary>The time between two passes.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly ReplicaGroupCommitter _committer;
    private readonly INodeLocator _locator;
    private readonly ILogger<ReplicaExpirationSweepService> _log;
    private readonly string _nodeId;
    private readonly ILocalCacheSnapshotReader<object?> _reader;

    /// <summary>Initializes a new instance of the <see cref="ReplicaExpirationSweepService" /> class.</summary>
    /// <param name="committer">The committer of the owned group; its clock decides expiry and times the passes.</param>
    /// <param name="reader">The stored entries.</param>
    /// <param name="locator">The key owner lookup.</param>
    /// <param name="nodeId">This node identifier, the owner of the owned group's keys.</param>
    /// <param name="log">Logger of the failed passes.</param>
    internal ReplicaExpirationSweepService(
        ReplicaGroupCommitter committer,
        ILocalCacheSnapshotReader<object?> reader,
        INodeLocator locator,
        string nodeId,
        ILogger<ReplicaExpirationSweepService> log)
    {
        ArgumentNullException.ThrowIfNull(committer);
        ArgumentNullException.ThrowIfNull(reader);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(log);
        _committer = committer;
        _reader = reader;
        _locator = locator;
        _nodeId = nodeId;
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

    /// <summary>Runs one pass: expires the owned keys whose deadline passed, at most <see cref="MaxPerPass" />, stopping at the first failure.</summary>
    /// <param name="cancellationToken">Cancellation token of the host.</param>
    /// <returns>The number of keys expired, or found live again, before the pass ended.</returns>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken" /> was canceled.</exception>
    internal async Task<int> SweepOnceAsync(CancellationToken cancellationToken)
    {
        var expired = 0;
        var now = _committer.Clock.GetUtcNow().UtcDateTime;
        try
        {
            await foreach (var (key, entry) in _reader.EnumerateLiveAsync(cancellationToken).ConfigureAwait(false))
            {
                if (expired == MaxPerPass)
                    break;

                if (entry.ExpiresUtc is not { } deadline || deadline.Ticks > now.Ticks)
                    continue;

                // A key of a group this node only follows is expired by its own leader, never here.
                if (!string.Equals(_locator.GetOwner(key.Namespace, key.Key), _nodeId, StringComparison.Ordinal))
                    continue;

                _ = await _committer.ExpireAsync(key.Namespace, key.Key, cancellationToken).ConfigureAwait(false);
                expired++;
            }
        }
        catch (Exception error) when (!cancellationToken.IsCancellationRequested)
        {
            ServerLog.ReplicaExpirationSweepFailed(_log, expired, error);
        }

        return expired;
    }

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _committer.Clock);
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
