using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>An RF=3 group owner repairs a follower its log cannot catch up by a state transfer, holds its log meanwhile, and lets the hold go however the pass ends.</summary>
public sealed class ReplicaStateTransfersTests : IsolatedStorageTestBase
{
    private const string GroupId = "n1";
    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    private string OwnerDir => Path.Join(Dir, "owner");

    /// <summary>
    /// A follower behind the compaction boundary is transferred to, caught up from the transferred index, and admitted: the pass sends the
    /// state once, no entry before it, and the pin is released after the admission.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactedFollowerIsTransferred(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, FollowerMode.Match, cancellationToken);
        var compaction = await leader.CompactAsync(cancellationToken);
        leader.Gateway.Set("n3", FollowerMode.Mismatch);
        await leader.CommitAsync("k4", cancellationToken);
        var transfer = new ScriptedTransferGateway { OnEnd = Bind.To<Leader, Action>(leader, static l => () => l.Gateway.Set("n3", FollowerMode.Behind, 4)) };
        leader.Gateway.Set("n3", FollowerMode.Behind, 2);

        _ = await Assert.That(await leader.Committer.VerifyReplicasAsync(cancellationToken)).IsEqualTo(ReplicaVerification.Pending);
        var before = EntriesSentTo(leader.Gateway, "n3");
        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), cancellationToken);

        _ = await Assert.That(compaction).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);
        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(string.Join(',', transfer.Calls)).IsEqualTo("begin,snapshot,wipe,entries,end");
        _ = await Assert.That(string.Join(',', transfer.Batches)).IsEqualTo("3");
        _ = await Assert.That(transfer.Aborts).IsEqualTo(0);
        _ = await Assert.That(leader.Registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(EntriesSentTo(leader.Gateway, "n3")).IsEqualTo(before);
        _ = await Assert.That(leader.Committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>A follower that answers incomplete is transferred to on its first answer, with no entry sent before; one that answers unseeded gets none.</summary>
    /// <param name="transferred">Whether the follower answers incomplete, and so a transfer is expected; otherwise it answers unseeded.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task IncompleteIsTransferredAndUnseededIsNot(bool transferred, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, transferred ? FollowerMode.Incomplete : FollowerMode.Unseeded, cancellationToken);
        var transfer = new ScriptedTransferGateway { OnEnd = Bind.To<Leader, Action>(leader, static l => () => l.Gateway.Set("n3", FollowerMode.Behind, 3)) };
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        var before = EntriesSentTo(leader.Gateway, "n3");

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsEqualTo(transferred);
        _ = await Assert.That(!transfer.Calls.IsEmpty).IsEqualTo(transferred);
        _ = await Assert.That(EntriesSentTo(leader.Gateway, "n3")).IsEqualTo(before);
    }

    /// <summary>The pin is released on every way a pass can end: a refusal at begin or mid-stream, a fault, a higher term, a cancellation, and a dispose.</summary>
    /// <param name="exit">How the pass ends.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments("begin")]
    [Arguments("entries")]
    [Arguments("fault")]
    [Arguments("term")]
    [Arguments("cancel")]
    [Arguments("dispose")]
    public async Task PinIsReleasedOnEveryExit(string exit, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, FollowerMode.Incomplete, cancellationToken);
        var transfer = exit switch
        {
            "begin" => new ScriptedTransferGateway { RefuseAt = "begin" },
            "entries" => new ScriptedTransferGateway { RefuseAt = "entries" },
            "fault" => new ScriptedTransferGateway { FailAt = "entries" },
            "term" => new ScriptedTransferGateway { RefuseAt = "begin", RefusalCode = RefusalCodes.StaleTerm, ReplyTerm = 9UL },
            _ => new ScriptedTransferGateway { HoldAt = "entries" },
        };
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        using var pass = new CancellationTokenSource();
        var run = leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), pass.Token);
        var heldMidStream = false;
        var holds = string.Equals(exit, "cancel", StringComparison.Ordinal) || string.Equals(exit, "dispose", StringComparison.Ordinal);
        if (holds)
        {
            await transfer.Held.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            heldMidStream = leader.Committer.TransferPin.IsHeld;
            if (string.Equals(exit, "cancel", StringComparison.Ordinal))
                await pass.CancelAsync();
            else
                await leader.Committer.DisposeAsync();
        }

        try
        {
            _ = await run.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        catch (Exception exception) when (exception is OperationCanceledException or ObjectDisposedException)
        {
            // The pass ended by the cancellation or by the dispose that was asked for.
        }

        _ = await Assert.That(leader.Committer.TransferPin.IsHeld).IsFalse();
        _ = await Assert.That(heldMidStream).IsEqualTo(holds);
        _ = await Assert.That(leader.Registry.EligibilityFor(GroupId).CanCountInWriteQuorum(2)).IsFalse();
    }

    /// <summary>A step refused as busy ends the pass with no abort, and a transport fault sends exactly one.</summary>
    /// <param name="fault">Whether the transfer fails in transport; otherwise the follower answers busy.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task BusyEndsQuietlyAndFaultAbortsOnce(bool fault, CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, FollowerMode.Incomplete, cancellationToken);
        var transfer = fault ? new ScriptedTransferGateway { FailAt = "snapshot" } : new ScriptedTransferGateway { RefuseAt = "begin" };
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), await TransfersAsync(transfer, cancellationToken), cancellationToken);

        _ = await Assert.That(admitted).IsFalse();
        _ = await Assert.That(transfer.Aborts).IsEqualTo(fault ? 1 : 0);
    }

    /// <summary>A follower that refused a transfer is not sent another until a growing cooldown ends.</summary>
    /// <param name="refusalCode">The refusal that starts a cooldown.</param>
    /// <param name="step">The transfer step that is refused.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(RefusalCodes.JournalFull, "begin")]
    public async Task RefusalStartsCooldown(string refusalCode, string step, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenTimedLeaderAsync(registry, FollowerMode.Incomplete, clock, cancellationToken);
        var transfer = new ScriptedTransferGateway { RefuseAt = step, RefusalCode = refusalCode };
        var transfers = await TransfersAsync(transfer, cancellationToken);

        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var first = transfer.TransferIdCount;
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var insideFirst = transfer.TransferIdCount;
        clock.Advance(TimeSpan.FromSeconds(31));
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var afterFirst = transfer.TransferIdCount;
        clock.Advance(TimeSpan.FromSeconds(31));
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var insideSecond = transfer.TransferIdCount;
        clock.Advance(TimeSpan.FromSeconds(31));
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
        var afterSecond = transfer.TransferIdCount;

        _ = await Assert.That((first, insideFirst, afterFirst, insideSecond, afterSecond)).IsEqualTo((1, 1, 2, 2, 3));
    }

    /// <summary>Cooldowns expire at their boundary, stop growing at ten minutes, and log the refusal reason.</summary>
    /// <param name="refusalCode">The refusal that starts a cooldown.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(RefusalCodes.JournalFull)]
    public async Task CooldownIsCappedAndExplained(string refusalCode, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenTimedLeaderAsync(registry, FollowerMode.Incomplete, clock, cancellationToken);
        var transfer = new ScriptedTransferGateway { RefuseAt = "begin", RefusalCode = refusalCode };
        var log = new EventRecordingLogger();
        var transfers = await TransfersAsync(transfer, log, cancellationToken);
        await CatchUpOnceAsync(leader, transfers, cancellationToken);

        int[] delays = [30, 60, 120, 240, 480, 600, 600];
        for (var i = 0; i < delays.Length; i++)
        {
            clock.Advance(TimeSpan.FromSeconds(delays[i]) - TimeSpan.FromTicks(1));
            await CatchUpOnceAsync(leader, transfers, cancellationToken);
            _ = await Assert.That(transfer.TransferIdCount).IsEqualTo(i + 1);
            clock.Advance(TimeSpan.FromTicks(1));
            await CatchUpOnceAsync(leader, transfers, cancellationToken);
            _ = await Assert.That(transfer.TransferIdCount).IsEqualTo(i + 2);
            _ = await Assert.That(leader.Committer.TransferPin.IsHeld).IsFalse();
        }

        _ = await Assert.That(log.Count(4087, refusalCode)).IsEqualTo(delays.Length + 1);
    }

    /// <summary>A completed transfer starts the cooldown over, so the next refusal waits the first period again, not a longer one.</summary>
    /// <param name="refusalCode">The refusal that starts a cooldown.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(RefusalCodes.JournalFull)]
    [Arguments(RefusalCodes.ChecksumMismatch)]
    public async Task CompletedTransferResetsCooldown(string refusalCode, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenTimedLeaderAsync(registry, FollowerMode.Incomplete, clock, cancellationToken);
        var transfer = new ScriptedTransferGateway { RefuseAt = "begin", RefusalCode = refusalCode };
        var transfers = await TransfersAsync(transfer, cancellationToken);
        var initial = string.Equals(refusalCode, RefusalCodes.ChecksumMismatch, StringComparison.Ordinal)
            ? new ReplicaReadinessOptions().InitialDelay
            : TimeSpan.FromSeconds(30);
        await CatchUpOnceAsync(leader, transfers, cancellationToken);
        clock.Advance(initial);
        await CatchUpOnceAsync(leader, transfers, cancellationToken);
        clock.Advance(initial * 2);
        transfer.RefuseAt = null;
        await CatchUpOnceAsync(leader, transfers, cancellationToken);
        transfer.RefuseAt = "begin";
        await CatchUpOnceAsync(leader, transfers, cancellationToken);
        var refusedAfterReset = transfer.Calls.Count;

        clock.Advance(initial);
        await CatchUpOnceAsync(leader, transfers, cancellationToken);

        _ = await Assert.That(transfer.Calls.Count).IsGreaterThan(refusedAfterReset).Because("The first cooldown period applies again after a transfer completed.");
    }

    /// <summary>A busy answer is a contact of the follower and a sign of progress for the election state of the leader; any other refusal is not.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyReplyCountsAsProgress(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        var election = registry.StateFor(GroupId);

        ReplicaStateTransfers.Post(election, 1, ReplicaTransferReply.Refused(1UL, RefusalCodes.TransferBusy));
        var afterOtherRefusal = election.TransferProgress.IsRecent(HangGuard);
        ReplicaStateTransfers.Post(election, 1, ReplicaTransferReply.Refused(1UL, RefusalCodes.JournalBusy));

        _ = await Assert.That((afterOtherRefusal, election.TransferProgress.IsRecent(HangGuard))).IsEqualTo((false, true));
    }

    /// <summary>A follower that stalls on a busy journal does not hold the others: the transfer to the healthy one runs in the same pass and admits it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BusyFollowerYieldsToTheOther(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenTimedLeaderAsync(registry, FollowerMode.Incomplete, clock, cancellationToken);
        leader.Gateway.Set("n2", FollowerMode.Incomplete);
        _ = leader.Registry.EligibilityFor(GroupId).TryDemote(1, 3UL);
        var transfer = new ScriptedTransferGateway { BusyNode = "n2", OnEnd = Bind.To<Leader, Action>(leader, static l => () => l.Gateway.Set("n3", FollowerMode.Behind, 3)) };
        transfer.BusyFor("begin", int.MaxValue);
        var transfers = await TransfersAsync(transfer, cancellationToken);
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);

        var admitted = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);

        _ = await Assert.That(admitted).IsTrue();
        _ = await Assert.That(leader.Registry.EligibilityFor(GroupId).StateFor(2)).IsEqualTo(ReplicaParticipantState.Ready);
        _ = await Assert.That(transfer.Aborts).IsEqualTo(1);
    }

    /// <summary>
    /// While the log is pinned compaction is skipped like a follower that is not ready, past the retention limit too, and it runs again once
    /// the pin is released.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionWaitsForThePin(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, FollowerMode.Match, cancellationToken);
        var policy = new ReplicaLogCompactionPolicy(long.MaxValue, 1);
        using var pin = leader.Committer.TransferPin.Begin();
        pin.Hold(3UL);

        var pinned = await leader.Committer.CompactOwnedLogAsync(policy, Durable(), cancellationToken);
        var pinnedPastLimit = await leader.Committer.CompactOwnedLogAsync(policy with { MaxRetainedBytes = 0L }, Durable(), cancellationToken);
        pin.Dispose();
        var released = await leader.Committer.CompactOwnedLogAsync(policy, Durable(), cancellationToken);

        _ = await Assert.That(pinned).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady);
        _ = await Assert.That(pinnedPastLimit).IsEqualTo(ReplicaLogCompactionOutcome.FollowerNotReady).Because("A held pin keeps the log past the retention limit too.");
        _ = await Assert.That(released).IsEqualTo(ReplicaLogCompactionOutcome.Compacted);
    }

    /// <summary>Nothing is captured, no pin is taken and no stream starts when nothing is applied or the pipeline of the follower was replaced.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CaptureRefusalsStartNothing(CancellationToken cancellationToken)
    {
        await using var registry = await OpenRegistryAsync(OwnerDir, cancellationToken);
        await using var leader = await OpenLeaderAsync(registry, FollowerMode.Match, cancellationToken, 0);
        using (await leader.Committer.Gate.LockAsync(cancellationToken))
            await leader.Committer.StartAsync(cancellationToken);

        var target = ThrowHelper.Required(leader.Committer.RunningPipeline, "The committer must be started.").CatchUpTargetFor(1);
        using var empty = leader.Committer.TransferPin.Begin();
        var (nothing, _) = await leader.Committer.CaptureForTransferAsync(target, Durable(), empty, cancellationToken);

        await leader.CommitAsync("k9", cancellationToken);
        leader.Committer.DropStartedState();
        await leader.CommitAsync("k10", cancellationToken);
        using var stale = leader.Committer.TransferPin.Begin();
        var (replaced, _) = await leader.Committer.CaptureForTransferAsync(target, Durable(), stale, cancellationToken);
        _ = leader.Registry.TryGetLog(GroupId, out var log);
        _ = log!.Idempotency.Reserve("client", "pending", [1], GroupRecordKind.UserMutation, 2UL, 1UL, GroupReserveKit.Unlogged);
        var fresh = ThrowHelper.Required(leader.Committer.RunningPipeline, "The committer must be started.").CatchUpTargetFor(1);
        using var unresolved = leader.Committer.TransferPin.Begin();
        var (outcome, _) = await leader.Committer.CaptureForTransferAsync(fresh, Durable(), unresolved, cancellationToken);

        _ = await Assert.That(nothing).IsEqualTo(ReplicaTransferOutcome.NothingApplied);
        _ = await Assert.That(replaced).IsEqualTo(ReplicaTransferOutcome.NotLeading);
        _ = await Assert.That(outcome).IsEqualTo(ReplicaTransferOutcome.UnresolvedOutcome);
        _ = await Assert.That(leader.Committer.TransferPin.IsHeld).IsFalse();
    }

    /// <summary>Every transfer outcome has a distinct stable name.</summary>
    [Test]
    public async Task EveryOutcomeIsNamed()
    {
        var names = new HashSet<string>(StringComparer.Ordinal);
        var values = Enum.GetValues<ReplicaTransferOutcome>();
        foreach (var value in values)
            _ = names.Add(ReplicaTransferOutcomeNames.Of(value));

        _ = await Assert.That(names.Count).IsEqualTo(values.Length);
    }

    private static int EntriesSentTo(ScriptedGateway gateway, string node)
    {
        var sent = 0;
        foreach (var append in gateway.Appends)
        {
            if (string.Equals(append.Node, node, StringComparison.Ordinal))
                sent++;
        }

        return sent;
    }

    private static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    private static async Task CatchUpOnceAsync(Leader leader, ReplicaStateTransfers transfers, CancellationToken cancellationToken)
    {
        _ = await leader.Committer.VerifyReplicasAsync(cancellationToken);
        _ = await leader.Committer.CatchUpFollowersAsync(Reporter(), transfers, cancellationToken);
    }

    private static ReplicaCatchUpReporter Reporter() => new(GroupId, NullLogger.Instance, null);

    private static Task<ReplicaStateTransfers> TransfersAsync(ScriptedTransferGateway gateway, CancellationToken cancellationToken) =>
        TransfersAsync(gateway, NullLogger<ReplicaStateTransfers>.Instance, cancellationToken);

    private static async Task<ReplicaStateTransfers> TransfersAsync(ScriptedTransferGateway gateway, ILogger<ReplicaStateTransfers> log, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2030, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var cache = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        for (var i = 1; i <= 3; i++)
            await cache.SetAsync(new CacheKey("cache", $"k{i}"), new NodeCacheEntry<object?>($"v{i}"), cancellationToken);

        var locator = new INodeLocatorCreateExpectations();
        _ = locator.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(GroupId);
        return new ReplicaStateTransfers(gateway, new ReplicaGroupEntries(cache, locator.Instance()), Durable(), new ReplicaTransferOptions(), log, null);
    }

    /// <summary>Opens the owner of group n1 with three committed and applied writes; n2 holds the log and n3 answers as scripted.</summary>
    /// <param name="registry">The open registry of the owner.</param>
    /// <param name="lagging">How n3 answers the leader.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="writes">The number of writes to commit.</param>
    /// <returns>The leader.</returns>
    private static Task<Leader> OpenLeaderAsync(ReplicaGroupRegistry registry, FollowerMode lagging, CancellationToken cancellationToken, int writes = 3) =>
        OpenTimedLeaderAsync(registry, lagging, null, cancellationToken, writes);

    /// <summary>Opens the owner like <see cref="OpenLeaderAsync" />, with the committer budget on a given clock.</summary>
    /// <param name="registry">The open registry of the owner.</param>
    /// <param name="lagging">How n3 answers the leader.</param>
    /// <param name="budgetClock">The clock of the committer budget, which the transfers use for their waits; the system clock when <see langword="null" />.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="writes">The number of writes to commit.</param>
    /// <returns>The leader.</returns>
    private static async Task<Leader> OpenTimedLeaderAsync(ReplicaGroupRegistry registry, FollowerMode lagging, TimeProvider? budgetClock, CancellationToken cancellationToken, int writes = 3)
    {
        var gateway = new ScriptedGateway();
        gateway.Set("n2", FollowerMode.Match);
        gateway.Set("n3", lagging);
        Leader? leader = null;
        try
        {
            leader = new Leader(registry, gateway, budgetClock);
            for (var i = 1; i <= writes; i++)
                await leader.CommitAsync($"k{i}", cancellationToken);

            return leader;
        }
        catch
        {
            if (leader != null)
                await leader.DisposeAsync();

            throw;
        }
    }

    /// <summary>The owner of a group with its committer and the follower transport.</summary>
    private sealed class Leader : IAsyncDisposable
    {
        internal Leader(ReplicaGroupRegistry registry, ScriptedGateway gateway, TimeProvider? budgetClock)
        {
            Registry = registry;
            Committer = budgetClock == null ? CreateCommitter(registry, gateway) : CreateCommitterOnBudgetClock(registry, gateway, budgetClock);
            Gateway = gateway;
        }

        internal ReplicaGroupCommitter Committer { get; }

        internal ScriptedGateway Gateway { get; }

        internal ReplicaGroupRegistry Registry { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();

        internal Task CommitAsync(string key, CancellationToken cancellationToken) =>
            Committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        /// <summary>Makes the applied index durable and drops the log through it, so a follower that lacks entries cannot be caught up from the log.</summary>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>A task that completes when the log is compacted.</returns>
        internal Task<ReplicaLogCompactionOutcome> CompactAsync(CancellationToken cancellationToken) =>
            Committer.CompactOwnedLogAsync(new ReplicaLogCompactionPolicy(long.MaxValue, 1), Durable(), cancellationToken);
    }
}
