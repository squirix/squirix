using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Two owners start their groups against each other: the calls that wake one another stay bounded, and a cluster whose members are all up still seeds without the clock moving.</summary>
[Immutable]
public sealed class ReplicaGroupBootstrapPeersTests : ServerUnitTestBase
{
    private static readonly StartedLifetime AlreadyStarted = new(new CancellationToken(true));
    private static readonly byte[] Fingerprint = [9, 8, 7];
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>The committers of a node that leads no group, which the service has nothing to wake in.</summary>
    private static readonly ReplicaGroupCommitters NoLedGroups = new([], "n0", new INodeLocatorCreateExpectations().Instance(), TimeProvider.System);

    /// <summary>
    /// Two owners that both wait for a third member that is down call each other once: a call of a peer that is not the member waited for wakes
    /// nobody, however many times the peers call, and the clock does not move.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OwnersWaitingForThirdMemberStayQuiet(CancellationToken cancellationToken)
    {
        using var firstDir = new TempDirectory("squirix-bootstrap-peers-down-1");
        using var secondDir = new TempDirectory("squirix-bootstrap-peers-down-2");
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var network = new PeerBootstrapNetwork(Reported(Fingerprint));
        await using var first = await CreateAsync(firstDir, "n1", (network, clock, ["n1", "n2", "n3"]), cancellationToken);
        await using var second = await CreateAsync(secondDir, "n2", (network, clock, ["n1", "n2", "n3"]), cancellationToken);
        await first.StartAsync(cancellationToken);
        await second.StartAsync(cancellationToken);

        await network.WaitForStatusCallsAsync("n1", 2, cancellationToken);
        await network.WaitForStatusCallsAsync("n2", 2, cancellationToken);
        await first.StopAsync(cancellationToken);
        await second.StopAsync(cancellationToken);

        _ = await Assert.That((network.StatusCallsOf("n1"), network.StatusCallsOf("n2"))).IsEqualTo((2, 2));
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>Two owners that wait for each other and never agree stop calling each other once each has found the same state twice, while the clock does not move.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MutualWaitIsBoundedWithoutClock(CancellationToken cancellationToken)
    {
        using var firstDir = new TempDirectory("squirix-bootstrap-peers-mismatch-1");
        using var secondDir = new TempDirectory("squirix-bootstrap-peers-mismatch-2");
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var network = new PeerBootstrapNetwork(Reported([1]));
        await using var first = await CreateAsync(firstDir, "n1", (network, clock, ["n1", "n2"]), cancellationToken);
        await using var second = await CreateAsync(secondDir, "n2", (network, clock, ["n1", "n2"]), cancellationToken);
        await first.StartAsync(cancellationToken);
        await second.StartAsync(cancellationToken);

        await network.WaitForStatusCallsAsync("n1", 2, cancellationToken);
        await network.WaitForStatusCallsAsync("n2", 2, cancellationToken);
        await first.StopAsync(cancellationToken);
        await second.StopAsync(cancellationToken);

        _ = await Assert.That(network.StatusCallsOf("n1")).IsLessThanOrEqualTo(2);
        _ = await Assert.That(network.StatusCallsOf("n2")).IsLessThanOrEqualTo(2);
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    /// <summary>The arrival of the last member wakes both owners, and both seed their groups, with no clock advance.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LastMemberArrivalSeedsBothOwners(CancellationToken cancellationToken)
    {
        using var firstDir = new TempDirectory("squirix-bootstrap-peers-seed-1");
        using var secondDir = new TempDirectory("squirix-bootstrap-peers-seed-2");
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var network = new PeerBootstrapNetwork(Reported(Fingerprint));
        await using var first = await CreateAsync(firstDir, "n1", (network, clock, ["n1", "n2", "n3"]), cancellationToken);
        await using var second = await CreateAsync(secondDir, "n2", (network, clock, ["n1", "n2", "n3"]), cancellationToken);
        await first.StartAsync(cancellationToken);
        await second.StartAsync(cancellationToken);
        await network.WaitForStatusCallsAsync("n1", 2, cancellationToken);
        await network.WaitForStatusCallsAsync("n2", 2, cancellationToken);

        network.Arrive("n3");
        await first.Completion.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        await second.Completion.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That((await first.Log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That((await second.Log.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Complete);
        _ = await Assert.That(clock.GetUtcNow()).IsEqualTo(DateTimeOffset.UnixEpoch);
    }

    private static ReplicaMemberStatus Reported(byte[] fingerprint) => new(GroupMemberState.Unseeded, true, fingerprint, 1UL, string.Empty);

    private static async Task<Owner> CreateAsync(string dir, string groupId, (PeerBootstrapNetwork Network, FakeTimeProvider Clock, string[] Members) world, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, groupId, GroupComposition.Create(groupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await log.OpenAsync(cancellationToken);
        var contact = new ReplicaPeerContact();
        world.Network.Up(groupId, contact);
        var header = new ReplicaRpcHeader(groupId, Fingerprint, 1UL, 0UL, string.Empty, groupId);
        var bootstrap = new ReplicaGroupBootstrap(log, world.Network.GatewayOf(groupId), world.Members, in header, TimeSpan.FromHours(1));
        var service = new ReplicaGroupBootstrapService(bootstrap, (contact, AlreadyStarted, NoLedGroups, new ReplicaApplySignal()), NullLogger<ReplicaGroupBootstrapService>.Instance, world.Clock);
        return new Owner(log, service);
    }

    /// <summary>An owner under test: its log and the service of its first start.</summary>
    [Immutable]
    private sealed class Owner : IAsyncDisposable
    {
        private readonly ReplicaGroupBootstrapService _service;

        internal Owner(FollowerLog log, ReplicaGroupBootstrapService service)
        {
            Log = log;
            _service = service;
        }

        internal Task Completion => _service.ExecuteTask ?? Task.CompletedTask;

        internal FollowerLog Log { get; }

        public async ValueTask DisposeAsync()
        {
            await _service.StopAsync(CancellationToken.None).ConfigureAwait(false);
            _service.Dispose();
            await Log.DisposeAsync().ConfigureAwait(false);
        }

        internal Task StartAsync(CancellationToken cancellationToken) => _service.StartAsync(cancellationToken);

        internal Task StopAsync(CancellationToken cancellationToken) => _service.StopAsync(cancellationToken);
    }
}
