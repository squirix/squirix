using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Exercises authenticated transfer requests against a seeded replica and its durable term.</summary>
public sealed class LeadershipTransferWireTests : ServerUnitTestBase
{
    /// <summary>Rejected topology and destination claims do not consume a subsequent valid attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HintRequiresTopologyTargetAndTerm(CancellationToken cancellationToken)
    {
        await using var follower = await SquirixReplicationFollowerAdapterTests.CreateFollowerScopeAsync("group", true, null, null, cancellationToken, true);
        var h = follower.Header;
        var append = await follower.Adapter.AppendReplicaEntriesAsync(new AppendReplicaEntriesRequest { Header = h }, follower.Context);
        _ = await Assert.That(append.Success).IsTrue();
        var state = follower.Registry.StateFor(h.GroupId);
        state.SetElectionDriven(true);
        state.BecomeFollower(h.Term, false);
        state.ApplyBlock.Assess();

        async Task<bool> AcceptAsync(string target)
        {
            return (await follower.Adapter.TransferLeadershipAsync(new LeadershipTransferRequest { Header = h, Target = target }, follower.Context)).Granted;
        }

        _ = await Assert.That(await AcceptAsync("other")).IsFalse();
        h.ConfigurationGeneration++;
        _ = await Assert.That(await AcceptAsync("node")).IsFalse();
        h.ConfigurationGeneration--;
        var fingerprint = h.TopologyFingerprint;
        h.TopologyFingerprint = default;
        _ = await Assert.That(await AcceptAsync("node")).IsFalse();
        h.TopologyFingerprint = fingerprint;
        h.Term++;
        _ = await Assert.That(await AcceptAsync("node")).IsFalse();
        h.Term--;
        _ = await Assert.That(await AcceptAsync("node")).IsTrue();
        _ = await Assert.That(await AcceptAsync("node")).IsFalse();
    }

    /// <summary>Only the transfer opcode with the matching context waives recent contact.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OrdinaryPreVoteIgnoresTransferClaims(CancellationToken cancellationToken)
    {
        await using var follower = await SquirixReplicationFollowerAdapterTests.CreateFollowerScopeAsync("group", true, null, null, cancellationToken, true);
        var h = follower.Header;
        var append = await follower.Adapter.AppendReplicaEntriesAsync(new AppendReplicaEntriesRequest { Header = h }, follower.Context);
        _ = await Assert.That(append.Success).IsTrue();
        var state = follower.Registry.StateFor(h.GroupId);
        state.SetElectionDriven(true);
        state.BecomeFollower(h.Term, false);
        h.LeaderNodeId = "former";
        state.ObserveLeaderContact(h.LeaderNodeId, h.Term);
        h.Term++;
        var ballot = new ReplicaVoteRequest { Header = h };
        var ordinary = await follower.Adapter.PreVoteAsync(ballot, follower.Context);
        _ = await Assert.That(ordinary.Granted).IsFalse();
        h.ConfigurationGeneration++;
        var mismatch = await follower.Adapter.PreVoteAsync(ballot, follower.Context, true);
        _ = await Assert.That(mismatch.Granted).IsFalse();
        h.ConfigurationGeneration--;
        var scoped = await follower.Adapter.PreVoteAsync(ballot, follower.Context, true);
        _ = await Assert.That(scoped.Granted).IsTrue();
        _ = await Assert.That(scoped.Term).IsEqualTo(7UL);
    }
}
