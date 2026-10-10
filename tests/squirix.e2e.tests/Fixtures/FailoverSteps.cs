using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Steps the leader stop and restart tests share: their cluster options, the members of a group, the recovery probe and the final reads.</summary>
internal static class FailoverSteps
{
    /// <summary>The nodes of the three-node cluster; with three replicas each of them is a member of every group.</summary>
    internal static readonly string[] ThreeNodes = ["nodeA", "nodeB", "nodeC"];

    /// <summary>The nodes of the five-node cluster, in the order that defines the members of a group: an owner and the nodes after it, wrapping around.</summary>
    internal static readonly string[] FiveNodes = ["nodeA", "nodeB", "nodeC", "nodeD", "nodeE"];

    /// <summary>The longest wait of a step the test does not measure: a stable leader before the fault, convergence and a quiet group after it.</summary>
    internal static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    /// <summary>The longest time from the start of a fault until a write and a read through a surviving member succeed on a CI agent.</summary>
    internal static readonly TimeSpan RecoveryBound = TimeSpan.FromSeconds(30);

    /// <summary>Gets the members of a group other than one node.</summary>
    /// <param name="members">The members.</param>
    /// <param name="nodeId">The node left out.</param>
    /// <returns>The other members, in order.</returns>
    internal static string[] Except(string[] members, string nodeId) => Array.FindAll(members, id => !string.Equals(id, nodeId, StringComparison.Ordinal));

    /// <summary>Gets the members of the group an owner heads: the owner and the next nodes of the ring, wrapping around.</summary>
    /// <param name="nodes">The nodes of the cluster, in ring order.</param>
    /// <param name="ownerId">The owner, which names the group.</param>
    /// <param name="replicaCount">The replica factor, which is the size of the group.</param>
    /// <returns>The members, the owner first.</returns>
    internal static string[] MembersOf(string[] nodes, string ownerId, int replicaCount)
    {
        var members = new string[replicaCount];
        var start = Array.IndexOf(nodes, ownerId);
        for (var i = 0; i < replicaCount; i++)
            members[i] = nodes[(start + i) % nodes.Length];

        return members;
    }

    /// <summary>Finds register keys of one group: keys whose owner, and so whose group, is the given node.</summary>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="groupId">The group, named after its owner.</param>
    /// <param name="prefix">The key prefix.</param>
    /// <param name="count">The number of keys.</param>
    /// <returns>Distinct keys of the group.</returns>
    internal static string[] KeysOf(string cacheName, string groupId, string prefix, int count) => KeysOf(KeyOwnerHelper.ThreeNode, cacheName, groupId, prefix, count);

    /// <summary>Finds register keys of one group on the ring of a given cluster.</summary>
    /// <param name="ring">The key ring of the cluster.</param>
    /// <param name="cacheName">The cache name.</param>
    /// <param name="groupId">The group, named after its owner.</param>
    /// <param name="prefix">The key prefix.</param>
    /// <param name="count">The number of keys.</param>
    /// <returns>Distinct keys of the group.</returns>
    internal static string[] KeysOf(KeyOwnerHelper ring, string cacheName, string groupId, string prefix, int count)
    {
        var keys = new string[count];
        for (var i = 0; i < count; i++)
            keys[i] = ring.FindKeyOwnedBy(cacheName, groupId, string.Create(CultureInfo.InvariantCulture, $"{prefix}-{i}-"));

        return keys;
    }

    /// <summary>The cluster options of a failover test: the given number of replicas, automatic failover with quorum reads, and the pull request tier timing.</summary>
    /// <param name="testName">The test name, which seeds the election jitter.</param>
    /// <param name="nodeClock">An optional clock per node.</param>
    /// <param name="fabric">An optional fabric the nodes dial each other through, which must outlive the cluster.</param>
    /// <param name="services">An optional hook that registers additional services on a node, receiving the node identifier.</param>
    /// <param name="replicaCount">The replica factor.</param>
    /// <returns>The options.</returns>
    internal static MultiNodeStartOptions Options(
        string testName,
        Func<string, TimeProvider?>? nodeClock = null,
        PartitionFabric? fabric = null,
        Action<string, IServiceCollection>? services = null,
        int replicaCount = 3)
    {
        return new MultiNodeStartOptions
        {
            ReplicaCount = replicaCount,
            Failover = true,
            ElectionTiming = FailoverTiming.For(testName),
            NodeClock = nodeClock,
            PartitionFabric = fabric,
            ServicesConfigure = services,
        };
    }

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

    /// <summary>Tells whether a register reached a value; a call the cluster cannot serve yet counts as not reached, so a wait for progress does not fail on a transient refusal.</summary>
    /// <param name="state">The cache to read through, the register key and the value to reach.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> when the register holds at least the value.</returns>
    internal static async ValueTask<bool> ReachedAsync((ICache<long> Reader, string Key, long Value) state, CancellationToken cancellationToken)
    {
        try
        {
            return (await state.Reader.GetValueAsync(state.Key, cancellationToken)).Value >= state.Value;
        }
        catch (RpcException exception) when (exception.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded)
        {
            return false;
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
