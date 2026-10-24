using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaPipelineTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A run of entries is appended to the owned log as one request and queued on every follower sender in log order.</summary>
public sealed class ReplicaGroupCommitRunTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A run of entries goes to the owned log as one request that carries all of them in order and names the predecessor of the first.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunIsOneLogRequestWithEveryEntry(CancellationToken cancellationToken)
    {
        var requests = new List<FollowerLogAppendRequest>();
        var pipeline = CreateLoggedPipeline(requests);

        await pipeline.AppendLocalAsync([Mutation(1), Mutation(2), Mutation(3)], cancellationToken);

        _ = await Assert.That(requests.Count).IsEqualTo(1);
        var request = requests[0];
        _ = await Assert.That(request.Entries.Length).IsEqualTo(3);
        for (var i = 0; i < request.Entries.Length; i++)
            _ = await Assert.That(request.Entries.Span[i].LogIndex).IsEqualTo(ulong.CreateChecked(i + 1));

        _ = await Assert.That((request.PrevLogIndex, request.PrevLogTerm)).IsEqualTo((0UL, 0UL));
        _ = await Assert.That(pipeline.NextLogIndex).IsEqualTo(4UL);
    }

    /// <summary>A run that is not dense never reaches the log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NonDenseRunIsRefusedBeforeTheLog(CancellationToken cancellationToken)
    {
        var requests = new List<FollowerLogAppendRequest>();
        var pipeline = CreateLoggedPipeline(requests);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(pipeline.AppendLocalAsync([Mutation(1), Mutation(3)], cancellationToken).AsTask());

        _ = await Assert.That(requests.Count).IsEqualTo(0);
    }

    /// <summary>Every entry of a run is queued on every sender with its own predecessor: each follower answers each entry, and its requests chain without a gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RunEntriesKeepTheirPredecessor(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            var entries = new[] { Mutation(1), Mutation(2), Mutation(3) };

            await pipeline.AppendLocalAsync(entries, cancellationToken);

            _ = await Assert.That(await AcknowledgedIndexesAsync(pipeline, 1, entries, cancellationToken)).IsEqualTo("123");
            _ = await Assert.That(await AcknowledgedIndexesAsync(pipeline, 2, entries, cancellationToken)).IsEqualTo("123");
            _ = await Assert.That(ChainedThrough(routing, "n2")).IsEqualTo(3UL);
            _ = await Assert.That(ChainedThrough(routing, "n3")).IsEqualTo(3UL);
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    /// <summary>The fan-out serves every entry of the last run, and refuses one of an earlier run.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FanOutServesOnlyTheLastRun(CancellationToken cancellationToken)
    {
        await using var followers = await OpenFollowersAsync(Dir, cancellationToken);
        var routing = new FollowerLogRoutingGateway(followers);
        await using var registry = await OpenRegistryAsync(Path.Join(Dir, "owner"), cancellationToken);
        var pipeline = await CreatePipelineAsync(registry, routing, cancellationToken);
        try
        {
            await pipeline.AppendLocalAsync([Mutation(1), Mutation(2)], cancellationToken);
            var lastRun = new[] { Mutation(3), Mutation(4) };
            await pipeline.AppendLocalAsync(lastRun, cancellationToken);

            var earlier = pipeline.AppendFollowerAsync(1, Mutation(2), cancellationToken).AsTask();
            _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(earlier);
            _ = await Assert.That(await AcknowledgedIndexesAsync(pipeline, 1, lastRun, cancellationToken)).IsEqualTo("34");
        }
        finally
        {
            _ = await pipeline.CloseAsync();
        }
    }

    private static async Task<string> AcknowledgedIndexesAsync(ReplicaGroupCommitPipeline pipeline, int replicaIndex, PreparedReplicaMutation[] entries, CancellationToken cancellationToken)
    {
        var indexes = string.Empty;
        foreach (var entry in entries)
        {
            var acknowledgement = await pipeline.AppendFollowerAsync(replicaIndex, entry, cancellationToken).AsTask().WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            indexes = $"{indexes}{acknowledgement.LogIndex}";
        }

        return indexes;
    }

    /// <summary>Walks the requests a follower received and returns the last index when each one names the last index of the one before as its predecessor.</summary>
    /// <param name="routing">The gateway that recorded the requests.</param>
    /// <param name="node">The follower.</param>
    /// <returns>The last index reached, or zero at the first request that does not chain.</returns>
    private static ulong ChainedThrough(FollowerLogRoutingGateway routing, string node)
    {
        var reached = 0UL;
        foreach (var arrival in routing.Arrivals)
        {
            if (!string.Equals(arrival.Node, node, StringComparison.Ordinal))
                continue;
            if (arrival.PrevLogIndex != reached)
                return 0UL;

            reached = arrival.LastIndex;
        }

        return reached;
    }

    private static ReplicaGroupCommitPipeline CreateLoggedPipeline(List<FollowerLogAppendRequest> requests)
    {
        var log = new IFollowerLogCreateExpectations();
        _ = log.Setups.AppendAsync(Arg.Any<FollowerLogAppendRequest>(), Arg.Any<CancellationToken>()).Callback((request, _) =>
        {
            requests.Add(request);
            return Task.FromResult(new FollowerLogAppendResult(true, string.Empty, request.PrevLogIndex + ulong.CreateChecked(request.Entries.Length), 1UL));
        });
        _ = log.Setups.Readiness.Gets().ReturnValue(FollowerLogReadiness.Ready);
        var status = default(FollowerLogStatus);
        var lagging = new ReplicaLaggingFollowers(GroupId, new ReplicaEligibility(3), new ReplicaRepairQueue(3), NullLogger.Instance);
        return new ReplicaGroupCommitPipeline(new ReplicaGroupApplier(new StubCache(), NullLogger.Instance), log.Instance(), [], (GroupId, 0), lagging, in status, 1);
    }
}
