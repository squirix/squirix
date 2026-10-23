using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.LocalCache;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Removes the entries no read touches once the local clock passes their deadline.</summary>
/// <remarks>
/// Registered on hosts where the local clock decides expiry. A read already drops an expired entry, but one nobody reads would stay in
/// memory and keep counting toward the estimated bytes that memory admission checks. Every pass takes the due items from the deadline index
/// of the cache, so it costs in proportion to the expired entries, not to the stored ones. The removal is memory-only, like the drop on
/// read: recovery, snapshots and compaction already treat an expired entry as absent.
/// </remarks>
internal sealed class LocalExpirationSweepService : BackgroundService
{
    /// <summary>The time between two passes.</summary>
    internal static readonly TimeSpan Interval = TimeSpan.FromSeconds(10);

    private readonly ILocalCacheExpiredKeys _cache;
    private readonly TimeProvider _clock;
    private readonly ILogger<LocalExpirationSweepService> _log;

    /// <summary>Initializes a new instance of the <see cref="LocalExpirationSweepService" /> class.</summary>
    /// <param name="cache">The deadline index of the cache to sweep.</param>
    /// <param name="clock">The host clock that times the passes.</param>
    /// <param name="log">Logger of the stop line.</param>
    internal LocalExpirationSweepService(ILocalCacheExpiredKeys cache, TimeProvider clock, ILogger<LocalExpirationSweepService> log)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(log);
        _cache = cache;
        _clock = clock;
        _log = log;
    }

    /// <summary>Runs one pass: removes the entries whose deadline passed.</summary>
    /// <returns>The number of entries removed.</returns>
    internal int SweepOnce() => _cache.RemoveExpired();

    /// <inheritdoc />
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(Interval, _clock);
        await using var stop = timer.DisposeOnCancel(stoppingToken).ConfigureAwait(false);
        while (await timer.WaitForNextTickAsync(CancellationToken.None).ConfigureAwait(false))
            _ = SweepOnce();

        if (stoppingToken.IsCancellationRequested)
            ServerLog.LocalExpirationSweepStopped(_log);
    }
}
