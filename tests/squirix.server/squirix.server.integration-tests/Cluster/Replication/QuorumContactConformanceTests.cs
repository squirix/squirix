using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.ProtocolModel;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.TestKit.Replication;
using Squirix.Testing;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Compares the production probe, readiness and election paths with the bounded contact model.</summary>
public sealed class QuorumContactConformanceTests
{
    private const string Group = "contact-trace";
    private const int Term = 2;
    private static readonly TimeSpan TickDuration = TimeSpan.FromMilliseconds(250);

    /// <summary>Mismatching replicas sustain leadership until their contacts expire while remaining ineligible for writes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProductionProbeExpiryMatchesModel(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-contact-trace");
        string[] groups = [Group];
        string[] members = ["n1", "n2", "n3", "n4", "n5"];
        byte[] fingerprint = [9, 8, 7];
        var time = new FakeTimeProvider();
        var options = new ElectionTimerOptions { ElectionTimeout = TickDuration * QuorumContactState.ContactWindow, MaxJitter = TimeSpan.Zero, JitterSeed = 4UL };
        await SeededLogs.SeedGroupsAsync(dir, groups, cancellationToken);
        await using var registry = new ReplicaGroupRegistry(dir, groups, members.Length, fingerprint, 1UL, NullLoggerFactory.Instance, new FollowerLogOptions
        {
            TimeProvider = TimeProvider.System,
            ShutdownTimeProvider = TimeProvider.System,
        })
        {
            Election = options,
            ElectionClock = time,
        };
        await registry.OpenAsync(cancellationToken);
        _ = await Assert.That(registry.TryGetLog(Group, out var log)).IsTrue();
        _ = await log!.ObserveTermAsync(Term, cancellationToken);

        var header = new ReplicaRpcHeader(Group, fingerprint, 1UL, Term, members[0], members[0]);
        var state = registry.StateFor(Group);
        var driver = new ReplicaGroupElection(state, log, new IReplicaVoteGatewayCreateExpectations().Instance(), CreateLeadership(), members, in header);
        _ = await driver.StepAsync(cancellationToken);
        _ = state.BecomeLeader(Term);
        _ = await Assert.That(state.GrantAuthority(Term)).IsTrue();

        var eligibility = registry.EligibilityFor(Group);
        ReplicaReadinessProbe.UnverifyFollowers(eligibility, 0);
        var leader = await log.GetStatusAsync(cancellationToken);
        ReplicaReadinessProbe.MarkLeaderReady(eligibility, 0, in leader, fingerprint, 1UL);
        time.Advance(TickDuration);
        var model = QuorumContactState.Start(members.Length).Advance();

        var probe = new ReplicaVerificationProbe(registry, new Locator(members), new ProbeGateway(), (Group, members[0]), fingerprint, 1UL, NullLogger.Instance);
        var snapshot = await probe.ProbeAsync(log, Term, false, cancellationToken);
        var results = snapshot.Probed;
        _ = await Assert.That(results.Length).IsEqualTo(members.Length);
        ReplicaReadinessProbe.ApplyAll(eligibility, 0, results, in leader, fingerprint, 1UL, null);
        _ = await Assert.That((results[1].Kind, results[2].Kind, results[3].Kind, results[4].Kind))
                        .IsEqualTo((ReplicaProbeKind.LogMismatch, ReplicaProbeKind.LogMismatch, ReplicaProbeKind.Unreachable, ReplicaProbeKind.Unreachable));
        model = model.Receive(new QuorumProbeReply(1, Term, Term, QuorumProbeVerdict.LogMismatch))
                     .Receive(new QuorumProbeReply(2, Term, Term, QuorumProbeVerdict.LogMismatch));

        time.Advance(TickDuration);
        model = model.Advance().CheckQuorum();
        var retained = await driver.StepAsync(cancellationToken);
        _ = await Assert.That(retained.Event).IsEqualTo(ElectionEvent.None);
        _ = await Assert.That((state.HasAuthority, eligibility.HasWriteMajority(), eligibility.CanCountInWriteQuorum(1), eligibility.CanCountInWriteQuorum(2)))
                        .IsEqualTo((model.Role == NodeRole.Leader, model.HasWriteQuorum, model.CanCountInWriteQuorum(1), model.CanCountInWriteQuorum(2)));

        time.Advance(TickDuration);
        model = model.Advance().CheckQuorum();
        var expired = await driver.StepAsync(cancellationToken);
        _ = await Assert.That(expired.Event).IsEqualTo(ElectionEvent.SteppedDown);
        _ = await Assert.That((state.Role == ReplicaGroupRole.Follower, state.HasAuthority)).IsEqualTo((model.Role == NodeRole.Follower, false));
    }

    private static IReplicaLeadership CreateLeadership()
    {
        var leadership = new IReplicaLeadershipCreateExpectations();
        _ = leadership.Setups.HeartbeatAsync(Group, Arg.Any<CancellationToken>()).ReturnValue(Task.CompletedTask);
        _ = leadership.Setups.ApplyProgress(Group).ReturnValue(default);
        _ = leadership.Setups.AppendProgress(Group).ReturnValue(default);
        _ = leadership.Setups.RetireAsync(Group, Arg.Any<CancellationToken>()).ReturnValue(Task.FromResult(true));
        return leadership.Instance();
    }

    private sealed class Locator : IReplicaGroupLocator
    {
        private readonly string[] _members;

        internal Locator(string[] members)
        {
            _members = members;
        }

        public int ReplicaCount => _members.Length;

        public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination) => _members.AsSpan().CopyTo(destination);
    }

    private sealed class ProbeGateway : IReplicaRpcGateway
    {
        public Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken) =>
            string.Equals(nodeId, "n2", StringComparison.Ordinal) || string.Equals(nodeId, "n3", StringComparison.Ordinal)
                ? Task.FromResult(new FollowerLogAppendResult(false, RefusalCodes.LogMismatch, Term, 0UL))
                : Task.FromException<FollowerLogAppendResult>(new TimeoutException());
    }
}
