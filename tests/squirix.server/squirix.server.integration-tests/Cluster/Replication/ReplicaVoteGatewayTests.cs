using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
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
        var options = new IntegrationStartOptions { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = "replica-vote-gateway" };
        await using var cluster = await StartClusterAsync(CandidateId, VoterId, "node-c", options, cancellationToken);
        var candidate = cluster[CandidateId];
        await ReplicaGroupFollowers.AwaitVerifiedAsync(candidate, cancellationToken);
        var voterLog = GroupLog(cluster[VoterId]);
        var before = await voterLog.GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(CandidateId, before.TopologyFingerprint, before.ConfigurationGeneration, before.CurrentTerm + 2UL, CandidateId, CandidateId);
        var gateway = candidate.GetRequiredService<IReplicaVoteGateway>();

        var preVote = await gateway.PreVoteAsync(VoterId, header, before.LastLogIndex, before.LastLogTerm, cancellationToken);
        var vote = await gateway.RequestVoteAsync(VoterId, header, before.LastLogIndex, before.LastLogTerm, cancellationToken);

        _ = await Assert.That(preVote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.NotReady, 0UL));
        _ = await Assert.That(vote).IsEqualTo(new FollowerLogVoteResult(false, RefusalCodes.NotReady, 0UL));
        var after = await voterLog.GetStatusAsync(cancellationToken);
        _ = await Assert.That(after.CurrentTerm).IsEqualTo(before.CurrentTerm);
        _ = await Assert.That(after.VotedFor).IsEqualTo(before.VotedFor);
    }

    private static IFollowerLog GroupLog(ITestNodeHost host) =>
        host.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(CandidateId, out var log) ? log
            : throw new InvalidOperationException($"The node does not serve group {CandidateId}.");
}
