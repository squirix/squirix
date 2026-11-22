using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Wire.Replication.SquirixReplicationAdapterTestHelpers;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

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

        var response = await fixture.Adapter.PreVoteAsync(request, fixture.CreateHttpContext());

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

        var response = await fixture.Adapter.RequestVoteAsync(request, fixture.CreateHttpContext());

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

        var preVote = await fixture.Adapter.PreVoteAsync(new ReplicaVoteRequest { Header = header }, fixture.CreateHttpContext());
        var vote = await fixture.Adapter.RequestVoteAsync(new ReplicaVoteRequest { Header = header }, fixture.CreateHttpContext());

        _ = await Assert.That(preVote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(RefusalCodes.NotReady);
    }

    /// <summary>Verifies that vote requests without an envelope header are rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteMissingHeaderIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);

        var preVote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(fixture.Adapter.PreVoteAsync(new ReplicaVoteRequest(), fixture.CreateHttpContext()), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var vote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(fixture.Adapter.RequestVoteAsync(new ReplicaVoteRequest(), fixture.CreateHttpContext()), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(preVote.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(vote.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
    }

    /// <summary>Verifies that vote requests whose sender differs from the peer certificate are unauthenticated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VoteSenderMismatchIsRejectedAsync(CancellationToken cancellationToken)
    {
        using var fixture = await CreateAdapterAsync(cancellationToken);
        var request = new ReplicaVoteRequest { Header = CreateValidHeader("node-b") };

        var preVote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(fixture.Adapter.PreVoteAsync(request, fixture.CreateHttpContext()), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var vote = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(fixture.Adapter.RequestVoteAsync(request, fixture.CreateHttpContext()), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(preVote.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);
        _ = await Assert.That(vote.StatusCode).IsEqualTo(FrameStatus.Unauthenticated);
    }
}
