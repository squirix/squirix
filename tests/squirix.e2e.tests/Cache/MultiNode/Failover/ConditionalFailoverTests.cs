using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cache.MultiNode.Failover;

/// <summary>
/// Conditional operations keep the result of their first execution across a leader change: every acknowledged TryAdd, Update, Remove and
/// GetOrAdd returns what one execution in order would return, no client operation is committed twice, and acknowledged effects survive.
/// </summary>
public sealed class ConditionalFailoverTests : EndToEndTestBase
{
    private const string CacheName = "conditional-failover";

    /// <summary>The group under test, named after its owner; the test finds its leader at run time.</summary>
    private const string Group = "nodeA";

    /// <summary>The number of chains completed before the leader shuts down, so the fault hits running chains.</summary>
    private const int ChainsBeforeFault = 4;

    private const int ChainsPerWorker = 6;

    /// <summary>The most keys a worker may try: a chain that fails leaves its key, and the worker goes on with a new one.</summary>
    private const int KeysPerWorker = 40;

    private const int Workers = 4;

    /// <summary>
    /// Workers run chains of conditional operations, each on a fresh key of the group, while its leader shuts down abruptly. A chain that
    /// fails leaves its key ambiguous and is dropped; every chain that completes must see the results of one execution of each call.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Timeout(120_000)]
    [ParallelLimiter<FailoverLimit>]
    public async Task ConditionalOpsApplyOnceAcrossFailover(CancellationToken cancellationToken)
    {
        const string testName = nameof(ConditionalOpsApplyOnceAcrossFailover);
        await using var cluster = await HostedCluster.StartThreeNodeAsync(testName, FailoverSteps.Options(testName), true, cancellationToken);
        var probe = new ClusterLeaderProbe<ClusterStartOptions>(cluster.Cluster);
        var (former, formerTerm) = await probe.WaitForStableLeaderAsync(Group, FailoverSteps.ThreeNodes, FailoverSteps.Bound, cancellationToken);
        var survivors = FailoverSteps.Except(FailoverSteps.ThreeNodes, former);
        var cache = await (await cluster.ConnectClientAsync(survivors[0], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var checker = await (await cluster.ConnectClientAsync(survivors[1], cancellationToken)).GetCacheAsync<long>(CacheName, cancellationToken);
        var stream = new ConditionalStream(cache, probe.Ledger(Group));
        var workers = new Task[Workers];
        for (var i = 0; i < workers.Length; i++)
            workers[i] = stream.RunAsync(i, cancellationToken);

        var running = Task.WhenAll(workers);
        _ = await Task.WhenAny(stream.Progressed, running).WaitAsync(FailoverSteps.Bound, TimeProvider.System, cancellationToken);
        await cluster.AbruptShutdownNodeAsync(former);
        await cluster.StopNodeAsync(former);
        await running;
        var (_, term) = await probe.WaitForStableLeaderAsync(Group, survivors, FailoverSteps.Bound, cancellationToken);
        var lost = await FindLostAsync(checker, stream.Completed, cancellationToken);
        var report = await GroupLogAudit.RunAsync(cluster.Cluster, Group, survivors, FailoverSteps.Bound, cancellationToken);

        _ = await Assert.That(stream.Violations).IsEmpty().Because(stream.Summary());
        _ = await Assert.That(lost).IsEmpty().Because(stream.Summary());
        _ = await Assert.That(stream.Completed.Count).IsEqualTo(Workers * ChainsPerWorker).Because(stream.Summary());
        _ = await Assert.That(term).IsGreaterThan(formerTerm);
        _ = await Assert.That(report.ClientEntries).IsGreaterThan(0);
    }

    /// <summary>Reads the key of every completed chain after the failover; each must still hold the value its last GetOrAdd added.</summary>
    /// <param name="checker">The cache to read through, on a surviving node.</param>
    /// <param name="keys">The keys of the completed chains.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>One line per key that lost its value.</returns>
    private static async Task<List<string>> FindLostAsync(ICache<long> checker, IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        var lost = new List<string>();
        var attempts = new List<EventualAttempt>();
        foreach (var key in keys)
        {
            var read = await Eventually.SucceedsAsync((Cache: checker, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), FailoverSteps.Bound, attempts, cancellationToken);
            if (read != new CacheValueResult<long>(true, 3L))
                lost.Add($"key {key}: read {read} after the failover, expected 3");
        }

        return lost;
    }

    /// <summary>Runs chains of conditional operations on fresh keys and records every result that differs from one execution of each call in order.</summary>
    /// <remarks>
    /// A chain is TryAdd 1 (added), TryAdd 9 (refused), Update 2 (updated), GetOrAdd 9 (gets 2), Remove (removed), Remove (absent), GetOrAdd 3
    /// (adds 3) and a read of 3. A retried call whose first execution committed, but that executed again, returns the second result, which
    /// differs. A chain whose call fails leaves its key in an unknown state and is dropped.
    /// </remarks>
    private sealed class ConditionalStream
    {
        private readonly ICache<long> _cache;
        private readonly List<string> _completed = [];
        private readonly Lock _gate = new();
        private readonly GroupAuthorityLedger<ClusterStartOptions> _ledger;
        private readonly TaskCompletionSource _progressed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly List<string> _violations = [];
        private int _dropped;

        internal ConditionalStream(ICache<long> cache, GroupAuthorityLedger<ClusterStartOptions> ledger)
        {
            _cache = cache;
            _ledger = ledger;
        }

        /// <summary>Gets the keys of the completed chains.</summary>
        internal IReadOnlyList<string> Completed
        {
            get
            {
                lock (_gate)
                    return [.. _completed];
            }
        }

        /// <summary>Gets a task that completes once <see cref="ChainsBeforeFault" /> chains completed.</summary>
        internal Task Progressed => _progressed.Task;

        /// <summary>Gets one line per result that differs from one execution.</summary>
        internal IReadOnlyList<string> Violations
        {
            get
            {
                lock (_gate)
                    return [.. _violations];
            }
        }

        /// <summary>Runs the chains of one worker until <see cref="ChainsPerWorker" /> of them completed.</summary>
        /// <param name="worker">The worker number, which names its keys.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>A task that completes once the worker completed its chains.</returns>
        /// <exception cref="InvalidOperationException">The worker dropped too many chains.</exception>
        internal async Task RunAsync(int worker, CancellationToken cancellationToken)
        {
            await Task.Yield();
            var completed = 0;
            for (var i = 0; i < KeysPerWorker && completed < ChainsPerWorker; i++)
            {
                var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, Group, string.Create(CultureInfo.InvariantCulture, $"chain-{worker}-{i}-"));
                if (await RunChainAsync(key, cancellationToken))
                {
                    completed++;
                    Complete(key);
                    continue;
                }

                _ = Interlocked.Increment(ref _dropped);
                await _ledger.UntilValueAsync((Cache: _cache, Key: key), static (s, token) => ServesAsync(s.Cache, s.Key, token), "the group serves again", cancellationToken);
            }

            if (completed < ChainsPerWorker)
                throw new InvalidOperationException($"Worker {worker} completed only {completed} chains on {KeysPerWorker} keys. {Summary()}");
        }

        /// <summary>Describes the stream in one line, for assertion messages.</summary>
        /// <returns>The numbers of completed and dropped chains, and of violations.</returns>
        internal string Summary()
        {
            lock (_gate)
                return string.Create(CultureInfo.InvariantCulture, $"{_completed.Count} completed and {Volatile.Read(ref _dropped)} dropped chains, {_violations.Count} violations");
        }

        private static async ValueTask<bool> ServesAsync(ICache<long> cache, string key, CancellationToken cancellationToken)
        {
            try
            {
                _ = await cache.GetValueAsync(key, cancellationToken);
                return true;
            }
            catch (RpcException)
            {
                return false;
            }
        }

        private void Complete(string key)
        {
            lock (_gate)
            {
                _completed.Add(key);
                if (_completed.Count >= ChainsBeforeFault)
                    _ = _progressed.TrySetResult();
            }
        }

        private bool Matches<T>(string key, string call, T expected, T actual)
        {
            if (EqualityComparer<T>.Default.Equals(expected, actual))
                return true;

            lock (_gate)
                _violations.Add($"key {key}: {call} returned {actual}, one execution returns {expected}");

            return false;
        }

        /// <summary>Runs one chain on a fresh key.</summary>
        /// <param name="key">The key, absent when the chain starts.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>
        /// <see langword="true" /> when every call returned, whether or not its result matched; <see langword="false" /> when a call failed,
        /// which leaves the key in an unknown state.
        /// </returns>
        private async Task<bool> RunChainAsync(string key, CancellationToken cancellationToken)
        {
            try
            {
                _ = Matches(key, "TryAdd 1", true, await _cache.TryAddAsync(key, 1L, cancellationToken: cancellationToken))
                    && Matches(key, "TryAdd 9", false, await _cache.TryAddAsync(key, 9L, cancellationToken: cancellationToken))
                    && Matches(key, "Update 2", true, await _cache.UpdateAsync(key, 2L, cancellationToken))
                    && Matches(key, "GetOrAdd 9", 2L, (await _cache.GetOrAddAsync(key, static (_, _) => Task.FromResult(9L), cancellationToken: cancellationToken)).Value)
                    && Matches(key, "Remove", true, await _cache.RemoveAsync(key, cancellationToken))
                    && Matches(key, "Remove again", false, await _cache.RemoveAsync(key, cancellationToken))
                    && Matches(key, "GetOrAdd 3", 3L, (await _cache.GetOrAddAsync(key, static (_, _) => Task.FromResult(3L), cancellationToken: cancellationToken)).Value)
                    && Matches(key, "Get", new CacheValueResult<long>(true, 3L), await _cache.GetValueAsync(key, cancellationToken));
                return true;
            }
            catch (Exception exception) when (exception is RpcException or CommitOutcomeUnknownException)
            {
                return false;
            }
        }
    }
}
