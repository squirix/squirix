using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Unit tests for the vote paths of <see cref="SquirixReplicationServiceAdapter" /> without a group registry.</summary>
[Immutable]
public sealed class SquirixReplicationVoteAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that PreVote without a group registry refuses with term zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreVoteReturnsNotReadyAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new ReplicaVoteRequest { Header = CreateValidHeader(), LastLogIndex = 3, LastLogTerm = 2 };

        var response = await fixture.Adapter.PreVote(request, new TestServerCallContext(null, fixture.CreateHttpContext()));

        _ = await Assert.That(response.Term).IsEqualTo(0UL);
        _ = await Assert.That(response.Granted).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that RequestVote without a group registry refuses with term zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RequestVoteReturnsNotReadyAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new ReplicaVoteRequest { Header = CreateValidHeader(), LastLogIndex = 3, LastLogTerm = 2 };

        var response = await fixture.Adapter.RequestVote(request, new TestServerCallContext(null, fixture.CreateHttpContext()));

        _ = await Assert.That(response.Term).IsEqualTo(0UL);
        _ = await Assert.That(response.Granted).IsFalse();
        _ = await Assert.That(response.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that a vote request answers without binding the claimed leader to the peer certificate.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteIgnoresLeaderNodeIdAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var header = CreateValidHeader();
        header.LeaderNodeId = "node-b";

        var preVote = await fixture.Adapter.PreVote(new ReplicaVoteRequest { Header = header }, new TestServerCallContext(null, fixture.CreateHttpContext()));
        var vote = await fixture.Adapter.RequestVote(new ReplicaVoteRequest { Header = header }, new TestServerCallContext(null, fixture.CreateHttpContext()));

        _ = await Assert.That(preVote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that vote requests without an envelope header are rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteMissingHeaderIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);

        var preVote = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            fixture.Adapter.PreVote(new ReplicaVoteRequest(), new TestServerCallContext(null, fixture.CreateHttpContext())));
        var vote = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            fixture.Adapter.RequestVote(new ReplicaVoteRequest(), new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(preVote.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(vote.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>Verifies that vote requests whose sender differs from the peer certificate are unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteSenderMismatchIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new ReplicaVoteRequest { Header = CreateValidHeader("node-b") };

        var preVote = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.PreVote(request, new TestServerCallContext(null, fixture.CreateHttpContext())));
        var vote = await NodeAsyncAssert.ThrowsAsync<RpcException>(fixture.Adapter.RequestVote(request, new TestServerCallContext(null, fixture.CreateHttpContext())));

        _ = await Assert.That(preVote.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
        _ = await Assert.That(vote.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
    }
}
