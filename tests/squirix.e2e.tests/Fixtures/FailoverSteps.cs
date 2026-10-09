using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Steps the leader stop and restart tests share: their cluster options, the members of a group, the recovery probe and the final reads.</summary>
internal static class FailoverSteps
{
    /// <summary>The nodes of the three-node cluster; with three replicas each of them is a member of every group.</summary>
    internal static readonly string[] ThreeNodes = ["nodeA", "nodeB", "nodeC"];

    /// <summary>The longest wait of a step the test does not measure: a stable leader before the fault, convergence and a quiet group after it.</summary>
    internal static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    /// <summary>The longest time from the start of a fault until a write and a read through a surviving member succeed on a CI agent.</summary>
    internal static readonly TimeSpan RecoveryBound = TimeSpan.FromSeconds(30);

    /// <summary>Gets the members of a group other than one node.</summary>
    /// <param name="members">The members.</param>
    /// <param name="nodeId">The node left out.</param>
    /// <returns>The other members, in order.</returns>
    internal static string[] Except(string[] members, string nodeId) => Array.FindAll(members, id => !string.Equals(id, nodeId, StringComparison.Ordinal));

    /// <summary>Finds register keys of one group: keys whose owner, and so whose group, is the given node.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="groupId">The group, named after its owner.</param>
    /// <param name="prefix">The key prefix.</param>
    /// <param name="count">The number of keys.</param>
    /// <returns>Distinct keys of the group.</returns>
    internal static string[] KeysOf(string cacheName, string groupId, string prefix, int count)
    {
        var keys = new string[count];
        for (var i = 0; i < count; i++)
            keys[i] = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(cacheName, groupId, string.Create(CultureInfo.InvariantCulture, $"{prefix}-{i}-"));

        return keys;
    }

    /// <summary>The cluster options of a failover test: three replicas, automatic failover with quorum reads, and the pull request tier timing.</summary>
    /// <param name="testName">The test name, which seeds the election jitter.</param>
    /// <param name="nodeClock">An optional clock per node.</param>
    /// <returns>The options.</returns>
    internal static MultiNodeStartOptions Options(string testName, Func<string, TimeProvider?>? nodeClock = null) => new()
    {
        ReplicaCount = 3,
        Failover = true,
        ElectionTiming = FailoverTiming.For(testName),
        NodeClock = nodeClock,
    };

    /// <summary>Reads every register once more after the workload, so an acknowledged write lost by the fault fails the history check.</summary>
    /// <param name="history">The history of the workload.</param>
    /// <param name="reader">The cache to read through; its node must have run through the fault.</param>
    /// <param name="keys">The register keys.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once every register was read.</returns>
    internal static async Task ReadFinalAsync(RegisterHistory history, ICache<long> reader, IReadOnlyList<string> keys, CancellationToken cancellationToken)
    {
        var attempts = new List<EventualAttempt>();
        foreach (var key in keys)
        {
            var start = Stopwatch.GetTimestamp();
            var read = await Eventually.SucceedsAsync((Cache: reader, Key: key), static (s, token) => s.Cache.GetValueAsync(s.Key, token), Bound, attempts, cancellationToken);
            history.RecordRead(new RegisterRead(key, start, Stopwatch.GetTimestamp(), read.Found ? read.Value : 0L));
        }
    }

    /// <summary>
    /// Writes a value and reads it back through a surviving member until both succeed, and measures the time from the start of the fault;
    /// an unknown commit outcome is retried, since writing the same value again is idempotent.
    /// </summary>
    /// <param name="cache">The cache to write through; its node must survive the fault.</param>
    /// <param name="key">The probe key, written by nothing else.</param>
    /// <param name="value">The value, above every value the key held before.</param>
    /// <param name="faultStarted">When the fault started, in <see cref="Stopwatch" /> ticks.</param>
    /// <param name="attempts">Receives one entry per attempt.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The value read back, and the time from the start of the fault until it was read.</returns>
    internal static async Task<(CacheValueResult<long> Read, TimeSpan Elapsed)> RecoverAsync(
        ICache<long> cache,
        string key,
        long value,
        long faultStarted,
        List<EventualAttempt> attempts,
        CancellationToken cancellationToken)
    {
        // The bound leaves room past the recovery bound, so a slow recovery fails the bound assertion with its timeline, not this wait.
        var read = await Eventually.SucceedsAsync(
            (Cache: cache, Key: key, Value: value),
            static async (s, token) =>
            {
                try
                {
                    await s.Cache.SetAsync(s.Key, s.Value, cancellationToken: token);
                }
                catch (CommitOutcomeUnknownException exception)
                {
                    throw new RpcException(new Status(StatusCode.Unavailable, "The commit outcome of the probe write is unknown; writing it again is idempotent.", exception));
                }

                return await s.Cache.GetValueAsync(s.Key, token);
            },
            RecoveryBound * 2,
            attempts,
            cancellationToken);
        return (read, Stopwatch.GetElapsedTime(faultStarted));
    }
}
