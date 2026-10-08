using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Vote and pre-vote calls travel the internal mTLS listener between RF=3 nodes and leave the voter log unchanged.</summary>
public sealed class ReplicaVoteGatewayTests : NodeIntegrationTestBase
{
    private const string CandidateId = "node-a";
    private const string VoterId = "node-b";

    /// <summary>With automatic failover off, a voter answers both calls with a refusal at term zero and keeps its durable term and vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GatewayVoteRoundTripReportsRefusal(CancellationToken cancellationToken)
    {
        var options = new IntegrationStartOptions
        {
            ReplicaCount = 3,
            UsePersistence = true,
            CleanTestDir = true,
            ExtraScope = "replica-vote-gateway",
            ServicesConfigure = SetManualMaintenance,
        };
        await using var cluster = await StartClusterAsync(CandidateId, VoterId, "node-c", options, cancellationToken);
        var candidate = cluster[CandidateId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(candidate, cancellationToken);
        var voter = cluster[VoterId];
        var voterLog = GroupLog(voter);
        var before = await voterLog.GetStatusAsync(cancellationToken);
        var metaPath = GroupStoragePaths.GetMetadataPath(voter.DataDir, CandidateId);
        var metaBefore = await File.ReadAllBytesAsync(metaPath, cancellationToken);
        var header = new ReplicaRpcHeader(CandidateId, before.TopologyFingerprint, before.ConfigurationGeneration, before.CurrentTerm + 2UL, CandidateId, CandidateId);
        var gateway = candidate.GetRequiredService<IReplicaVoteGateway>();

        var preVote = await gateway.PreVoteAsync(VoterId, header, before.LastLogIndex, before.LastLogTerm, cancellationToken);
        var vote = await gateway.RequestVoteAsync(VoterId, header, before.LastLogIndex, before.LastLogTerm, cancellationToken);

        _ = await Assert.That(preVote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.NotReady, 0UL));
        _ = await Assert.That(vote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.NotReady, 0UL));
        var after = await voterLog.GetStatusAsync(cancellationToken);
        _ = await Assert.That(after.CurrentTerm).IsEqualTo(before.CurrentTerm);
        _ = await Assert.That(after.VotedFor).IsEqualTo(before.VotedFor);
        var metaAfter = await File.ReadAllBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(metaBefore, metaAfter);
    }

    /// <summary>The vote gateway and the replication gateway are one instance, sharing its pooled channels.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteGatewayIsTheReplicationGateway(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(CandidateId, VoterId, new IntegrationStartOptions { FoundationOnly = true }, cancellationToken);
        var node = cluster[CandidateId];

        var replication = node.GetRequiredService<IReplicaRpcGateway>();
        var votes = node.GetRequiredService<IReplicaVoteGateway>();

        _ = await Assert.That(ReferenceEquals(replication, votes)).IsTrue();
    }

    /// <summary>Replaces the group log maintenance schedule, so no pass rewrites the voter metadata within the test.</summary>
    /// <param name="services">The node service collection.</param>
    private static void SetManualMaintenance(IServiceCollection services)
    {
        for (var i = services.Count - 1; i >= 0; i--)
        {
            if (services[i].ServiceType == typeof(ReplicaLogCompactionOptions))
                services.RemoveAt(i);
        }

        _ = services.AddSingleton(new ReplicaLogCompactionOptions { Interval = TimeSpan.FromDays(1) });
    }

    private static IFollowerLog GroupLog(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(CandidateId, out var log) ? log
            : throw new InvalidOperationException($"The node does not serve group {CandidateId}.");
}
