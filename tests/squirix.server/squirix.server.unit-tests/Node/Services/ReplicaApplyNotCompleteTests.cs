using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaApplyServiceTests;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// The apply loop applies nothing to a copy that is not complete, and applies its committed entries once the copy completes. The loops of
/// the two followed groups park on the clock once their first pass ended, and a loop that parks again has ended the pass a wake-up started,
/// so every check below follows a pass that already ran.
/// </summary>
[Immutable]
public sealed class ReplicaApplyNotCompleteTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A group whose copy is unseeded or incomplete applies none of its committed entries, while the other group goes on.</summary>
    /// <param name="incomplete">Whether the copy of group n2 is planted incomplete; otherwise it is unseeded.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task NotCompleteCopyAppliesNothing(bool incomplete, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-not-complete");
        await using var registry = await OpenAsync(dir, (incomplete ? GroupMemberState.Incomplete : GroupMemberState.Unseeded, false), cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var woken = clock.WhenTimersAsync(3);
            registry.ApplySignalFor("n2").Notify();
            await woken.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["m1"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That((appliers.For("n2").AppliedIndex, appliers.For("n3").AppliedIndex)).IsEqualTo((0UL, 1UL));
    }

    /// <summary>A pass over a copy that is not complete leaves the apply block as it is: the election gate denies such a node anyway.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotCompleteCopyKeepsTheApplyBlock(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-not-complete-block");
        await using var registry = await OpenAsync(dir, (GroupMemberState.Incomplete, false), cancellationToken);
        var block = registry.StateFor("n2").ApplyBlock;
        block.Block();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, new StubCache(), meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(block.IsBlocked).IsTrue();
    }

    /// <summary>A pass over a copy that is not complete leaves the group unassessed, so the pass after it becomes complete decides before the node campaigns.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NotCompleteCopyStaysUnassessed(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-not-complete-assessed");
        await using var registry = await OpenAsync(dir, (GroupMemberState.Incomplete, false), cancellationToken);
        var block = registry.StateFor("n2").ApplyBlock;
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, new StubCache(), meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(block.IsAssessed).IsFalse();
        _ = await Assert.That(block.IsBlocked).IsFalse();
    }

    /// <summary>An unseeded copy applies its committed entries once it becomes complete, and none before.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CopyAppliesOnceComplete(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-completes");
        await using var registry = await OpenAsync(dir, (GroupMemberState.Unseeded, false), cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        ulong before;
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            before = appliers.For("n2").AppliedIndex;
            var all = WhenAppliedAsync(cache, 3);
            _ = registry.TryGetLog("n2", out var log);
            await log!.CompleteBootstrapAsync(cancellationToken);
            registry.ApplySignalFor("n2").Notify();
            await all.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["m1", "k1", "k2"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That((before, appliers.For("n2").AppliedIndex)).IsEqualTo((0UL, 2UL));
    }

    /// <summary>An offered copy that a member's append completes applies its committed entries and the appended one, with no other wake-up.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OfferedCopyAppliesAfterMemberAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-offered");
        await using var registry = await OpenAsync(dir, (GroupMemberState.Unseeded, true), cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        ulong before;
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            before = appliers.For("n2").AppliedIndex;
            var all = WhenAppliedAsync(cache, 4);
            var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
            var appended = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k3", 3UL), 2UL, 3UL), cancellationToken);
            _ = await Assert.That(appended.Success).IsTrue().Because($"refused with '{appended.RefusalCode}'");
            await all.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        await SequenceAssert.EqualAsync(["m1", "k1", "k2", "k3"], cache.Applied.ToArray(), StringComparer.Ordinal);
        _ = await Assert.That((before, appliers.For("n2").AppliedIndex)).IsEqualTo((0UL, 3UL));
    }

    /// <summary>Opens a registry whose group n2 holds two committed entries under a copy of the given state and whose group n3 is complete.</summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="copy">The member state planted for group n2 and whether the record holds an offer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The opened registry.</returns>
    private static async Task<ReplicaGroupRegistry> OpenAsync(string dir, (GroupMemberState State, bool Offered) copy, CancellationToken cancellationToken)
    {
        await using (var seeded = await OpenRegistryAsync(dir, Groups, null, cancellationToken))
        {
            await SeedAsync(seeded, "n2", 2UL, cancellationToken, "k1", "k2");
            await SeedAsync(seeded, "n3", 1UL, cancellationToken, "m1");
        }

        return await PlantedCopyTestKit.ReopenWithStateAsync(dir, Groups, "n2", copy, cancellationToken);
    }
}
