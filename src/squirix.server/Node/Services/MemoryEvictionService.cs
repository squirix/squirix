using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.LocalCache;
using Squirix.Server.Logging;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Services;

/// <summary>Frees cache entries while the estimated resident size exceeds the node's high threshold.</summary>
internal sealed class MemoryEvictionService : NodeWorkerBase
{
    private readonly PhysicalCache<object?> _cache;
    private readonly IMemoryPressureStateEvaluator _evaluator;
    private readonly ILogicalNamespacedCache<object?> _local;
    private readonly ReplicaGroupCommitters? _groups;
    private readonly TimeProvider _clock;
    private readonly IServerLogger<MemoryEvictionService> _logger;
    private int _nextGroup;

    internal MemoryEvictionService(
        PhysicalCache<object?> cache,
        IMemoryPressureStateEvaluator evaluator,
        ILogicalNamespacedCache<object?> local,
        ReplicaGroupCommitters? groups,
        TimeProvider clock,
        IServerLogger<MemoryEvictionService> logger)
    {
        _cache = cache;
        _evaluator = evaluator;
        _local = local;
        _groups = groups;
        _clock = clock;
        _logger = logger;
    }

    internal async Task SweepAsync(CancellationToken token)
    {
        ILocalCacheStats stats = _cache;
        for (var i = 0; i < 256 && _evaluator.Evaluate(stats.EstimatedBytes) != PressureLevel.Normal; i++)
        {
            token.ThrowIfCancellationRequested();
            if (!await RemoveOneAsync(token).ConfigureAwait(false))
                return;
        }
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1), _clock);
        while (await timer.WaitForNextTickAsync(stoppingToken).ConfigureAwait(false))
        {
            try
            {
                await SweepAsync(stoppingToken).ConfigureAwait(false);
            }
            catch (Exception error) when (!stoppingToken.IsCancellationRequested)
            {
                _logger.Log(ServerLogLevel.Warning, new ServerLogEventId(0, "MemoryEvictionFailed"), "Memory eviction pass failed.", error, static (message, _) => message);
            }
        }
    }

    private async Task<bool> RemoveOneAsync(CancellationToken token)
    {
        if (_groups == null)
        {
            return _cache.SelectPressureVictim(string.Empty) is { } key &&
                await _local.RemoveAsync(Guid.NewGuid().ToString("N"), key.Namespace, key.Key, token).ConfigureAwait(false);
        }

        var leaders = _groups.LedCommitters;
        for (var i = 0; i < leaders.Count; i++)
        {
            if (_nextGroup >= leaders.Count)
                _nextGroup = 0;
            var leader = leaders[_nextGroup++];
            if (_groups.FindAuthorized(leader.GroupId) == leader && _cache.SelectPressureVictim(leader.GroupId) is { } candidate)
                return await leader.CommitRemoveAsync(Guid.NewGuid().ToString("N"), candidate.Namespace, candidate.Key, token).ConfigureAwait(false);
        }

        return false;
    }
}
