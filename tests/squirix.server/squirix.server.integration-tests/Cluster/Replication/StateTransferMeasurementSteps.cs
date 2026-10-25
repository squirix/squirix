using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The steps of the state transfer measurement that read or drive a node and do not belong to one phase.</summary>
internal static class StateTransferMeasurementSteps
{
    /// <summary>The cache the measurement writes.</summary>
    internal const string CacheName = "transfer-measure";

    /// <summary>The node that owns and leads the measured group.</summary>
    internal const string OwnerId = "node-a";

    private const int VerifiedSample = 1000;
    private const long BytesPerMegabyte = 1024L * 1024L;
    private const int SmallestLogMb = 1;

    /// <summary>Bounds every wait of the run; a large group takes minutes, so the bound only catches a stuck run.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromHours(3);

    /// <summary>Finds keys the owner owns.</summary>
    /// <param name="host">A node of the cluster.</param>
    /// <param name="count">The number of keys.</param>
    /// <returns>Distinct keys of the group.</returns>
    internal static string[] KeysOf(ITestNodeHost host, int count)
    {
        var locator = host.GetRequiredService<INodeLocator>();
        var keys = new string[count];
        var found = 0;
        var candidateIndex = 0;
        while (found < count)
        {
            var candidate = $"{CacheName}-{candidateIndex++}";
            if (string.Equals(locator.GetOwner(CacheName, candidate), OwnerId, StringComparison.Ordinal))
                keys[found++] = candidate;
        }

        return keys;
    }

    /// <summary>Gets the log of the measured group on a node.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The group log.</returns>
    /// <exception cref="InvalidOperationException">The node does not serve the group.</exception>
    internal static IFollowerLog Log(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(OwnerId, out var log) ? log : throw new InvalidOperationException($"The node does not serve group log {OwnerId}.");

    /// <summary>Gets the slot of a node in the replica group of the measured group.</summary>
    /// <param name="host">A node of the cluster.</param>
    /// <param name="nodeId">The node identifier.</param>
    /// <returns>The slot index.</returns>
    internal static int SlotOf(ITestNodeHost host, string nodeId)
    {
        var locator = host.GetRequiredService<IReplicaGroupLocator>();
        var members = new string[locator.ReplicaCount];
        locator.GetReplicaGroup(OwnerId, members);
        return Array.IndexOf(members, nodeId);
    }

    /// <summary>Reads how many log entries a follower is behind the owner.</summary>
    /// <param name="owner">The owner node.</param>
    /// <param name="follower">The follower node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The number of entries; zero when the follower holds the last one.</returns>
    internal static async ValueTask<long> LagAsync(ITestNodeHost owner, ITestNodeHost follower, CancellationToken cancellationToken)
    {
        var leader = (await Log(owner).GetStatusAsync(cancellationToken)).LastLogIndex;
        var held = (await Log(follower).GetStatusAsync(cancellationToken)).LastLogIndex;
        return long.CreateChecked(leader - Math.Min(leader, held));
    }

    /// <summary>Reads the number of entries a node holds.</summary>
    /// <param name="host">The node.</param>
    /// <returns>The entry count.</returns>
    internal static long EntryCount(ITestNodeHost host) => host.GetRequiredService<IMemoryUsageAccounting>().ReadEntryCount();

    /// <summary>Reads the live managed heap after a full collection.</summary>
    /// <returns>The bytes the managed heap holds.</returns>
    internal static long LiveManagedBytes() =>
#pragma warning disable S1215 // The measurement is the live heap, which only a full collection isolates from garbage.
        GC.GetTotalMemory(true);
#pragma warning restore S1215

    /// <summary>Compares the entry counts of the owner and the repaired follower, and a sample of their values.</summary>
    /// <param name="owner">The owner node.</param>
    /// <param name="follower">The repaired follower.</param>
    /// <param name="keys">The keys of the run.</param>
    /// <param name="count">The number of leading keys that were filled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The entry counts of both nodes and the number of sampled values that differ.</returns>
    internal static async Task<(long Owner, long Follower, int Mismatches)> VerifyAsync(
        ITestNodeHost owner,
        ITestNodeHost follower,
        string[] keys,
        int count,
        CancellationToken cancellationToken)
    {
        var mismatches = 0;
        var ownerCache = owner.GetRequiredService<PhysicalCache<object?>>();
        var followerCache = follower.GetRequiredService<PhysicalCache<object?>>();
        var step = Math.Max(1, count / VerifiedSample);
        for (var i = 0; i < count; i += step)
        {
            var key = new CacheKey(CacheName, keys[i]);
            var expected = await ownerCache.GetValueAsync(key, cancellationToken);
            var actual = await followerCache.GetValueAsync(key, cancellationToken);
            if (!expected.Found || !actual.Found || !SameText(expected.Value, actual.Value))
                mismatches++;
        }

        return (owner.GetRequiredService<IMemoryUsageAccounting>().ReadEntryCount(), follower.GetRequiredService<IMemoryUsageAccounting>().ReadEntryCount(), mismatches);
    }

    /// <summary>Compacts the owner log past the follower that is down, as the owner does once the log reaches its retention limit.</summary>
    /// <param name="owner">The owner node.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A description of the attempts and their duration.</returns>
    internal static async Task<string> CompactPastFollowerAsync(ITestNodeHost owner, CancellationToken cancellationToken)
    {
        var committer = ReplicaNodeCommitters.OwnCommitter(owner);
        var policy = new ReplicaLogCompactionPolicy(SmallestLogMb * BytesPerMegabyte, int.MaxValue, SmallestLogMb * BytesPerMegabyte);
        var coordinator = owner.GetRequiredService<IJournalCoordinator>();
        var attempts = new List<ReplicaLogCompactionOutcome>();
        var started = Stopwatch.GetTimestamp();
        await (Committer: committer, Policy: policy, Coordinator: coordinator, Attempts: attempts).WaitUntilValueAsync(
            static async (state, token) =>
            {
                var outcome = await state.Committer.CompactOwnedLogAsync(state.Policy, state.Coordinator, token);
                state.Attempts.Add(outcome);
                return outcome == ReplicaLogCompactionOutcome.CompactedPastFollower;
            },
            Bound,
            TimeProvider.System,
            cancellationToken);
        return string.Create(CultureInfo.InvariantCulture, $"{Stopwatch.GetElapsedTime(started).TotalSeconds:F1} s, {attempts.Count} attempt(s), last outcome {attempts[^1]}");
    }

    private static bool SameText(object? left, object? right) => left is string a && right is string b && string.Equals(a, b, StringComparison.Ordinal);
}
