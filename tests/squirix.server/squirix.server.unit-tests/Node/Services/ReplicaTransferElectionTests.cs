using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.LedGroupsTestKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// A leader by election repairs a follower it cannot catch up from its log without authority, and its transfer keeps it in place: the replies are
/// contacts, a reply of a higher term is observed, and a leadership that does not match the log sends nothing. Node n1 leads group n1 from slot 0.
/// </summary>
public sealed class ReplicaTransferElectionTests : ServerUnitTestBase
{
    private const string GroupId = "n1";
    private const int FollowerCaughtUpEventId = 4021;
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A one-off checksum refusal recovers on the next readiness retry, before the authority deadline and without changing term.</summary>
    /// <param name="step">The transfer step refused once.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("begin")]
    [Arguments("snapshot")]
    [Arguments("entries")]
    [Arguments("end")]
    public async Task ChecksumRetryRecoversBeforeDeadline(string step, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-checksum-recovery");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenCompactedAsync(dir, clock, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var transfer = new ScriptedTransferGateway
        {
            RefuseAt = step,
            RefusalCode = RefusalCodes.ChecksumMismatch,
            OnEnd = () => gateway.Set("n2", FollowerMode.Behind, 3),
        };
        await using var committers = new ReplicaGroupCommitters(
            _ => ElectedOnClock(registry, gateway, clock),
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);
        var state = registry.StateFor(GroupId);
        _ = registry.TryGetLog(GroupId, out var log);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1UL, 2UL, "n1", "n1");
        var election = new ReplicaGroupElection(state, log!, new IReplicaVoteGatewayCreateExpectations().Instance(), committers, ["n1", "n2", "n3"], in header);
        _ = await election.StepAsync(cancellationToken);
        _ = state.BecomeLeader(2UL);
        _ = await election.StepAsync(cancellationToken);
        var committer = committers.LedCommitters[0];
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);
        var transfers = await TransfersAsync(transfer, cancellationToken);
        var first = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        _ = await Assert.That((first, registry.StateFor(GroupId).HasAuthority)).IsEqualTo((false, false));

        transfer.RefuseAt = null;
        var retryAfter = new ReplicaReadinessOptions().InitialDelay;
        clock.Advance(retryAfter);
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);
        var repaired = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var authorized = await election.StepAsync(cancellationToken);

        _ = await Assert.That((repaired, authorized.Event)).IsEqualTo((true, ElectionEvent.Authorized));
        _ = await Assert.That((state.Term, state.Role, state.HasAuthority)).IsEqualTo((2UL, ReplicaGroupRole.Leader, true));
        _ = await Assert.That(retryAfter).IsLessThan(registry.StateFor(GroupId).Options.AuthorityTimeout);
        _ = await Assert.That(transfer.BeginsFor("n2")).IsEqualTo(2);
        _ = await Assert.That(committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>Repeated checksum refusals are paced per follower while another follower is repaired and admitted in the same pass.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChecksumRetryDoesNotHoldOtherFollowers(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-checksum-fairness");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenCompactedAsync(dir, clock, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Behind, 2);
        var transfer = new ScriptedTransferGateway
        {
            RefuseAt = "end",
            RefusalNode = "n2",
            RefusalCode = RefusalCodes.ChecksumMismatch,
            OnEnd = () => gateway.Set("n3", FollowerMode.Behind, 3),
        };
        await using var committer = ElectedOnClock(registry, gateway, clock);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(false, cancellationToken);
        var transfers = await TransfersAsync(transfer, cancellationToken);

        var admitted = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        _ = await committer.VerifyReplicasAsync(false, cancellationToken);
        _ = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(registry.EligibilityFor(GroupId).CanCountInWriteQuorum(1)).IsFalse();
        _ = await Assert.That((transfer.BeginsFor("n2"), transfer.BeginsFor("n3"))).IsEqualTo((1, 1));
        _ = await Assert.That(committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>Checksum retries back off independently of repair signals but never delay the next attempt beyond a readiness polling window.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChecksumRetryStaysWithinReadinessWindow(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("transfer-checksum-window");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenCompactedAsync(dir, clock, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var transfer = new ScriptedTransferGateway { RefuseAt = "end", RefusalCode = RefusalCodes.ChecksumMismatch };
        var transfers = await TransfersAsync(transfer, cancellationToken);
        await using var committer = ElectedOnClock(registry, gateway, clock);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        var options = new ReplicaReadinessOptions();
        var delay = options.InitialDelay;

        for (var attempt = 0; attempt < 8; attempt++)
        {
            _ = await committer.VerifyReplicasAsync(true, cancellationToken);
            _ = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
            _ = await Assert.That(transfer.BeginsFor("n2")).IsEqualTo(attempt + 1);
            clock.Advance(delay - TimeSpan.FromTicks(1));
            _ = await committer.VerifyReplicasAsync(true, cancellationToken);
            _ = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
            _ = await Assert.That(transfer.BeginsFor("n2")).IsEqualTo(attempt + 1);
            clock.Advance(TimeSpan.FromTicks(1));
            delay = TimeSpan.FromTicks(Math.Min(options.MaxDelay.Ticks, delay.Ticks * 2));
        }

        transfer.RefuseAt = null;
        transfer.OnEnd = () => gateway.Set("n2", FollowerMode.Behind, 3);
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);
        var repaired = await committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        _ = await Assert.That(repaired).IsTrue();
    }

    /// <summary>
    /// The only live follower needs a transfer: the leader sends it before it has authority, admits it, commits its leader-term entry and gets
    /// authority, all through the readiness service.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeaderTransfersBeforeAuthority(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-before-authority");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenCompactedAsync(dir, clock, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var (onEnd, create) = Bind.To<(ReplicaGroupRegistry Registry, ScriptedGateway Gateway), (Action OnEnd, Func<string, ReplicaGroupCommitter> Create)>(
            (registry, gateway),
            static s => (() => s.Gateway.Set("n2", FollowerMode.Behind, 3), group => Elected(s.Registry, group, s.Gateway)));
        var transfer = new ScriptedTransferGateway { OnEnd = onEnd };
        await using var committers = new ReplicaGroupCommitters(
            create,
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);
        var state = registry.StateFor(GroupId);
        state.SetElectionDriven(true);
        _ = state.BecomeLeader(2UL);
        var log = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(committers, log, TimeProvider.System, null, await TransfersAsync(transfer, cancellationToken));

        await service.StartAsync(cancellationToken);
        bool first;
        bool authorized;
        try
        {
            first = await committers.PromoteAsync(GroupId, 2UL, cancellationToken);
            await log.WhenLoggedAsync(FollowerCaughtUpEventId, "follower n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            authorized = await committers.PromoteAsync(GroupId, 2UL, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That((first, authorized)).IsEqualTo((false, true));
        _ = await Assert.That(string.Join(',', transfer.Calls)).IsEqualTo("begin,snapshot,wipe,entries,end");
        _ = await Assert.That(registry.EligibilityFor(GroupId).StateFor(1)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(state.Role).IsEqualTo(ReplicaGroupRole.Leader);
    }

    /// <summary>
    /// A leader of a group it does not own repairs the owner, back after the failover, by a transfer and names itself, not the owner, in every
    /// batch it sends after the transfer: node n1 leads group n2.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferToOwnerNamesLeadingNode(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-owner-leader");
        await using var registry = await OpenLedCompactedAsync(dir, cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var probe = new EndProbe(registry, gateway);
        var transfer = new ScriptedTransferGateway { OnEnd = probe.OnEnd };
        await using var committers = new ReplicaGroupCommitters(
            probe.Create,
            new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")),
            "n1",
            Owners(),
            TimeProvider.System);
        var log = new EventRecordingLogger();
        using var service = new ReplicaGroupReadinessService(committers, log, TimeProvider.System, null, await TransfersAsync(transfer, "n2", cancellationToken));

        await service.StartAsync(cancellationToken);
        try
        {
            _ = await committers.PromoteAsync("n2", 2UL, cancellationToken);
            await log.WhenLoggedAsync(FollowerCaughtUpEventId, "follower n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(string.Join(',', transfer.Calls)).IsEqualTo("begin,snapshot,wipe,entries,end");
        var afterTransfer = gateway.AppendLeaders.ToArray()[probe.BeforeEnd..];
        _ = await Assert.That(afterTransfer.Length).IsGreaterThan(0);
        foreach (var (node, leader) in afterTransfer)
            _ = await Assert.That((node, leader)).IsEqualTo(("n2", "n1"));
    }

    /// <summary>Every accepted reply of the transfer is a contact of the follower and a progress of the transfer, and a stalled transfer is neither.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferRepliesAreContactAndProgress(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-contact");
        var clock = new FakeTimeProvider();
        await using var registry = await OpenCompactedAsync(dir, clock, cancellationToken);
        var state = registry.StateFor(GroupId);
        var window = state.Options.ElectionTimeout;
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var seen = (Contact: false, Progress: false);
        var transfer = new ScriptedTransferGateway
        {
            HoldAt = "entries",
            OnEnd = () =>
            {
                seen = (state.HasQuorumContact(0, window), state.TransferProgress.IsRecent(window));
                gateway.Set("n2", FollowerMode.Behind, 3);
            },
        };
        await using var committer = Elected(registry, GroupId, gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);
        var pass = committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), cancellationToken);
        await transfer.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        clock.Advance(window * 3);
        var stalled = (state.HasQuorumContact(0, window), state.TransferProgress.IsRecent(window));
        transfer.Release();
        var admitted = await pass.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(stalled).IsEqualTo((false, false));
        _ = await Assert.That(seen).IsEqualTo((true, true));
    }

    /// <summary>A reply of a higher term ends the transfer, is observed by the election state, and releases the pin.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermReplyIsObserved(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-higher-term");
        await using var registry = await OpenCompactedAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind, 2);
        gateway.Set("n3", FollowerMode.Down);
        var transfer = new ScriptedTransferGateway { RefuseAt = "begin", RefusalCode = RefusalCodes.StaleTerm, ReplyTerm = 7UL };
        await using var committer = Elected(registry, GroupId, gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);

        var admitted = await committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(registry.StateFor(GroupId).HighestObservedTerm).IsEqualTo(7UL);
        _ = await Assert.That(string.Join(',', transfer.Calls)).IsEqualTo("begin");
        _ = await Assert.That(committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>Nothing is captured while the tenure does not lead the term of the log or the election state does not say leader in it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeadershipMustMatchTheLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-leadership");
        await using var registry = await OpenCompactedAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Behind);
        gateway.Set("n3", FollowerMode.Down);
        await using var committer = Elected(registry, GroupId, gateway);
        _ = await committer.PromoteAsync(2UL, cancellationToken);
        _ = await committer.VerifyReplicasAsync(true, cancellationToken);
        var target = ThrowHelper.Required(committer.RunningPipeline, "The committer must be started.").CatchUpTargetFor(1);
        using var pin = committer.TransferPin.Begin();
        var (leading, _) = await committer.CaptureForTransferAsync(target, Durable(), pin, cancellationToken);
        pin.Dispose();

        _ = registry.TryGetLog(GroupId, out var log);
        _ = await log!.ObserveTermAsync(3UL, cancellationToken);
        using var raised = committer.TransferPin.Begin();
        var (logPastTerm, _) = await committer.CaptureForTransferAsync(target, Durable(), raised, cancellationToken);

        _ = await Assert.That(leading).IsEqualTo(ReplicaTransferOutcome.Transferred);
        _ = await Assert.That(logPastTerm).IsEqualTo(ReplicaTransferOutcome.NotLeading);
        _ = await Assert.That(committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>A node that starts leading a group ends the transfer it receives for it first, which frees the apply driver the transfer holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeadershipEndsTheReceivedTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-leading");
        await using var harness = await ReplicaStateHarness.OpenAsync(dir, cancellationToken);
        _ = await harness.BeginAndUploadAsync("transfer-1", 1UL, cancellationToken);
        var lease = harness.Appliers.For(ReplicaStateHarness.GroupId).DriverLease;
        var held = lease.TryEnterPass();

        await harness.Receiver.ReleaseAsync(ReplicaStateHarness.GroupId, cancellationToken);
        await lease.LeadAsync(cancellationToken);
        lease.EndLeading();

        _ = await Assert.That(held).IsFalse();
        _ = harness.Registry.TryGetLog(ReplicaStateHarness.GroupId, out var log);
        _ = await Assert.That(log!.IsComplete).IsTrue().Because("The copy stays as it was.");
        var begin = await harness.Receiver.BeginAsync(ReplicaStateHarness.HeaderOf("transfer-2", 1UL), ReplicaStateHarness.BaselineOf(TransferFixtures.Image()), cancellationToken);
        _ = await Assert.That(begin.Success).IsTrue().Because("The group is free for another transfer.");
    }

    /// <summary>
    /// A promotion that meets the applier lease held, as by a transfer being received, is a retryable fault and not an exception that ends the
    /// election loop: it reports no authority, holds no tenure, and starts leading once the lease is free.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionRetriesWhileLeaseIsHeld(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-lease-held");
        await using var registry = await OpenCompactedAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", FollowerMode.Match);
        await using var committer = Elected(registry, GroupId, gateway);
        var lease = committer.Applier.DriverLease;
        _ = await Assert.That(await lease.TryLeadAsync(cancellationToken)).IsTrue();

        var whileHeld = await committer.PromoteAsync(2UL, cancellationToken);
        var tenureWhileHeld = committer.Tenure;
        lease.EndLeading();
        _ = await committer.PromoteAsync(2UL, cancellationToken);

        _ = await Assert.That(whileHeld).IsFalse();
        _ = await Assert.That(tenureWhileHeld).IsNull();
        _ = await Assert.That(committer.Tenure).IsNotNull().Because("The retry started the leadership.");
    }

    /// <summary>The step that runs before a leadership starts is given the group, and runs before the committer holds a tenure.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PromotionRunsTheStepBeforeLeading(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-transfer-before-leading");
        await using var registry = await OpenCompactedAsync(dir, new FakeTimeProvider(), cancellationToken);
        var gateway = new ScriptedGateway();
        var probe = new LeadingProbe(registry, gateway);
        probe.Set = new ReplicaGroupCommitters(probe.Create, new ReplicaLeaderTable(registry, "n1", OwnerRouters.Locator("n1", "n2", "n3")), "n1", Owners(), TimeProvider.System)
        {
            BeforeLeading = probe.BeforeLeading,
        };
        await using var committers = probe.Set;

        _ = await committers.PromoteAsync(GroupId, 2UL, cancellationToken);

        _ = await Assert.That((probe.Ended, probe.TenureDuring)).IsEqualTo((GroupId, false));
        _ = await Assert.That(committers.Leads(GroupId)).IsTrue();
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static ReplicaCatchUpReporter Reporter() => new(GroupId, NullLogger.Instance, null);

    private static ReplicaGroupCommitter Elected(ReplicaGroupRegistry registry, string groupId, IReplicaRpcGateway gateway)
    {
        var cache = new StubCache();
        return CreateElectedCommitter(registry, groupId, gateway, cache, new ReplicaGroupApplier(cache, NullLogger.Instance, groupId, "n1"));
    }

    private static ReplicaGroupCommitter ElectedOnClock(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, TimeProvider clock)
    {
        var cache = new StubCache();
        return CreateElectedBudgetCommitter(registry, GroupId, gateway, (cache, new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1")), clock);
    }

    private static Task<ReplicaStateTransfers> TransfersAsync(ScriptedTransferGateway gateway, CancellationToken cancellationToken) =>
        TransfersAsync(gateway, GroupId, cancellationToken);

    private static async Task<ReplicaStateTransfers> TransfersAsync(ScriptedTransferGateway gateway, string groupId, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        for (var i = 1; i <= 3; i++)
            await cache.SetAsync(new CacheKey("cache", $"k{i}"), new NodeCacheEntry<object?>($"v{i}"), cancellationToken);

        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(groupId);
        return new ReplicaStateTransfers(gateway, new ReplicaGroupEntries(cache, locator.Instance()), Durable(), new ReplicaTransferOptions(), NullLogger<ReplicaStateTransfers>.Instance, null);
    }

    /// <summary>
    /// Opens node n1 serving the groups with the log of group n2 holding three committed and applied writes of its owner in term one that were
    /// dropped from the log by compaction, and in term two, as after a failover to n1; the election state of the group leads term two and has no authority.
    /// </summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The registry.</returns>
    private static async Task<ReplicaGroupRegistry> OpenLedCompactedAsync(string dir, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, Groups, cancellationToken);
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = new FakeTimeProvider() };
        try
        {
            await registry.OpenAsync(cancellationToken);
            _ = registry.TryGetLog("n2", out var log);
            _ = await log!.ObserveTermAsync(1UL, cancellationToken);
            var factory = new ReplicaMutationFactory(new StubCache(), "n2", 1UL, TimeProvider.System, NullLogger.Instance);
            var entries = new FollowerLogEntry[3];
            for (var i = 0; i < entries.Length; i++)
            {
                var index = ulong.CreateChecked(i + 1);
                entries[i] = new FollowerLogEntry(index, 1UL, factory.PrepareSet(NewOperationId(), "cache", $"k{index}", Entry($"k{index}"), index).CanonicalPayload);
            }

            var appended = await log.AppendAsync(new FollowerLogAppendRequest("n2", 1UL, 0UL, 0UL, 3UL, entries), cancellationToken);
            if (!appended.Success)
                throw new InvalidOperationException($"The group log refused the committed entries: {appended.RefusalCode}.");

            await FollowerLogTestKit.MakeCommitDurableAsync(log, cancellationToken);
            var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, "n2", "n1");
            await applier.CatchUpAsync(log, 0UL, 3UL, cancellationToken);
            await applier.FlushAsync(log, Durable(), cancellationToken);
            _ = await log.CompactThroughAsync(3UL, cancellationToken);
            _ = await log.ObserveTermAsync(2UL, cancellationToken);
            var state = registry.StateFor("n2");
            state.SetElectionDriven(true);
            _ = state.BecomeLeader(2UL);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    /// <summary>
    /// Opens node n1 serving the groups with the log of group n1 holding three committed and applied writes that were dropped from the log by
    /// compaction, in term two, as after a leader change; the election state of the group runs on <paramref name="clock" />, leads term two and has no authority.
    /// </summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="clock">The clock of the election state.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The registry.</returns>
    private static async Task<ReplicaGroupRegistry> OpenCompactedAsync(string dir, FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, Groups, cancellationToken);
        var options = new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System };
        var registry = new ReplicaGroupRegistry(dir, Groups, 3, Fingerprint, 1, NullLoggerFactory.Instance, options) { ElectionClock = clock };
        try
        {
            await registry.OpenAsync(cancellationToken);
            var seeding = new ScriptedGateway();
            seeding.Set("n2", FollowerMode.Match);
            seeding.Set("n3", FollowerMode.Match);
            await using (var owner = CreateCommitter(registry, seeding))
            {
                for (var i = 1; i <= 3; i++)
                    await owner.CommitSetAsync(NewOperationId(), "cache", $"k{i}", Entry($"k{i}"), cancellationToken);

                await owner.FlushAppliedAsync(Durable(), cancellationToken);
            }

            _ = registry.TryGetLog(GroupId, out var log);
            var status = await log!.GetStatusAsync(cancellationToken);
            _ = await log.CompactThroughAsync(status.CommitIndex, cancellationToken);
            _ = await log.ObserveTermAsync(2UL, cancellationToken);
            var state = registry.StateFor(GroupId);
            state.SetElectionDriven(true);
            _ = state.BecomeLeader(2UL);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    [Mutable]
    private sealed class EndProbe
    {
        private readonly ScriptedGateway _gateway;
        private readonly ReplicaGroupRegistry _registry;

        internal EndProbe(ReplicaGroupRegistry registry, ScriptedGateway gateway)
        {
            _registry = registry;
            _gateway = gateway;
        }

        internal int BeforeEnd { get; private set; } = -1;

        internal ReplicaGroupCommitter Create(string group) => Elected(_registry, group, _gateway);

        internal void OnEnd()
        {
            BeforeEnd = _gateway.AppendLeaders.Count;
            _gateway.Set("n2", FollowerMode.Behind, 3);
        }
    }

    [Mutable]
    private sealed class LeadingProbe
    {
        private readonly ScriptedGateway _gateway;
        private readonly ReplicaGroupRegistry _registry;

        internal LeadingProbe(ReplicaGroupRegistry registry, ScriptedGateway gateway)
        {
            _registry = registry;
            _gateway = gateway;
            BeforeLeading = (group, _) =>
            {
                Ended = group;
                TenureDuring = Set!.Leads(group);
                return Task.CompletedTask;
            };
        }

        internal Func<string, CancellationToken, Task> BeforeLeading { get; }

        internal string? Ended { get; private set; }

        internal bool TenureDuring { get; private set; } = true;

        internal ReplicaGroupCommitters? Set { get; set; }

        internal ReplicaGroupCommitter Create(string group) => Elected(_registry, group, _gateway);
    }
}
