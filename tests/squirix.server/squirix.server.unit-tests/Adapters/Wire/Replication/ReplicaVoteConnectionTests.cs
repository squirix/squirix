using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Exercises election deadlines over a real loopback TLS connection.</summary>
public sealed class ReplicaVoteConnectionTests
{
    private static readonly ElectionTimerOptions Timing = new() { VoteRpcTimeout = TimeSpan.FromMilliseconds(250) };

    /// <summary>Lets the voter answer when TLS was held longer than the reply timeout.</summary>
    /// <param name="preVote">Whether to test a pre-vote or a vote.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task representing the test.</returns>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    [Timeout(15_000)]
    public async Task ColdHandshakeDoesNotConsumeReplyBudget(bool preVote, CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var clock = new FakeTimeProvider();
        var running = StartRoundAsync(pool, clock, preVote, cancellationToken);

        await using var stream = await peer.AcceptTlsAsync(() => clock.Advance(TimeSpan.FromMilliseconds(800)), cancellationToken);
        var (operation, _, _) = await NativePeerFixture.ReadAsync(stream, cancellationToken);
        _ = await Assert.That(operation).IsEqualTo(preVote ? PeerOperation.PreVote : PeerOperation.RequestVote);
        var response = new ReplicaVoteResponse { Granted = true, Term = preVote ? 1UL : 2UL };
        await NativePeerFixture.ReplyAsync(stream, response.ToByteArray(), cancellationToken);

        _ = await Assert.That(await running).IsEqualTo((2, 0UL));
    }

    /// <summary>Ends the round when the voter accepts TCP but never completes TLS.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task representing the test.</returns>
    [Test]
    [Timeout(15_000)]
    public async Task SilentHandshakeTimesOutBeforeBallot(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var clock = new FakeTimeProvider();
        var running = StartRoundAsync(pool, clock, true, cancellationToken);
        using var socket = await peer.AcceptTcpAsync(cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(3));

        _ = await Assert.That(await running).IsEqualTo((1, 0UL));
    }

    /// <summary>Times out a voter which reads the request and gives no response.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task representing the test.</returns>
    [Test]
    [Timeout(15_000)]
    public async Task SilentVoterTimesOutAfterHandshake(CancellationToken cancellationToken)
    {
        using var peer = new NativePeerFixture();
        await using var pool = peer.CreatePool();
        var clock = new FakeTimeProvider();
        var running = StartRoundAsync(pool, clock, true, cancellationToken);
        await using var stream = await peer.AcceptTlsAsync(cancellationToken);
        _ = await NativePeerFixture.ReadAsync(stream, cancellationToken);

        clock.Advance(Timing.VoteRpcTimeout);

        _ = await Assert.That(await running).IsEqualTo((1, 0UL));
    }

    private static Task<(int Granted, ulong Highest)> StartRoundAsync(
        IServerClientPool pool,
        FakeTimeProvider clock,
        bool preVote,
        CancellationToken cancellationToken)
    {
        string[] members = ["node-a", NativePeerFixture.Node];
        var state = new ReplicaGroupState(members.Length, Timing, clock);
        var header = new ReplicaRpcHeader("g", new byte[32], 1, 2, string.Empty, members[0]);
        return new ReplicaVoteRound(new ReplicaRpcGateway(pool), members, 0, state)
            .RunAsync(preVote, header, preVote ? 1UL : 2UL, (0, 0), cancellationToken);
    }
}
