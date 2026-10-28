using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaApplyServiceTests;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The apply loop of a follower makes the commit index the leader announced durable before it applies anything the index covers.</summary>
[Immutable]
public sealed class ReplicaApplyCommitTests : ServerUnitTestBase
{
    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A pass writes the announced commit index to the metadata of the log, and only then applies the entries it covers.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassMakesAnnouncedCommitDurable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-commit-durable");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var applied = WhenAppliedAsync(cache, 1);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            var appended = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k1", 1UL), 0UL, 1UL), cancellationToken);
            _ = await Assert.That(appended.Success).IsTrue().Because($"refused with '{appended.RefusalCode}'");
            await applied.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = registry.TryGetLog("n2", out var log);
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitIndex, appliers.For("n2").AppliedIndex)).IsEqualTo((1UL, 1UL));
        _ = await Assert.That(GroupMetadataStore.Read(GroupStoragePaths.GetMetadataPath(dir, "n2")).Metadata.CommitIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// Appends that announce the commit index publish no metadata, and the pass that follows them makes the latest announced index durable with one
    /// metadata publish however many appends it covers.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PassPublishesOnceForManyAppends(CancellationToken cancellationToken)
    {
        const int Appends = 5;
        using var dir = new TempDirectory("squirix-apply-commit-amortized");
        var hooks = new CountingFollowerLogFaultHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = hooks }, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        // The first append adopts the leader term, which publishes metadata once; only the appends after it are measured.
        var adopted = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k1", 1UL), 0UL, 1UL), cancellationToken);
        _ = await Assert.That(adopted.Success).IsTrue().Because($"refused with '{adopted.RefusalCode}'");
        var beforeAppends = hooks.Snapshot();
        for (var i = 2; i <= Appends; i++)
        {
            var key = "k" + i.ToString(System.Globalization.CultureInfo.InvariantCulture);
            var appended = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare(key, ulong.CreateChecked(i)), ulong.CreateChecked(i - 1), ulong.CreateChecked(i)), cancellationToken);
            _ = await Assert.That(appended.Success).IsTrue().Because($"refused with '{appended.RefusalCode}'");
        }

        var afterAppends = hooks.Snapshot();
        var applied = WhenAppliedAsync(cache, Appends);
        var firstPasses = clock.WhenTimersAsync(2);

        await service.StartAsync(cancellationToken);
        try
        {
            await firstPasses.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await applied.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        var afterPass = hooks.Snapshot();
        _ = await Assert.That(afterAppends.Since(in beforeAppends).MetaWrites).IsEqualTo(0L).Because("an accepted append publishes no metadata");
        _ = await Assert.That(afterPass.Since(in afterAppends).MetaWrites).IsEqualTo(1L).Because("one pass makes the announced commit index of every append durable once");
        _ = await Assert.That(appliers.For("n2").AppliedIndex).IsEqualTo(ulong.CreateChecked(Appends));
    }

    /// <summary>A pass that cannot write the announced commit index applies nothing and marks the apply blocked, counting the pending entries from the announced index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedCommitWriteBlocksTheApply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-commit-failed");
        var faults = new MetaFailureHooks();
        await using var registry = await OpenRegistryAsync(dir, Groups, new FollowerLogOptions { ShutdownTimeProvider = TimeProvider.System, TimeProvider = TimeProvider.System, FaultHooks = faults }, cancellationToken);
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var clock = new ParkingClock();
        using var service = new ReplicaApplyService(registry, appliers, LeadOwnGroup(registry, appliers), ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(), NullLogger<ReplicaApplyService>.Instance, clock);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());
        var appended = await follower.AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k1", 1UL), 0UL, 1UL), cancellationToken);
        _ = await Assert.That(appended.Success).IsTrue().Because($"refused with '{appended.RefusalCode}'");
        faults.Arm();
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

        _ = await Assert.That(cache.Applied.Count).IsEqualTo(0);
        _ = await Assert.That(registry.StateFor("n2").ApplyBlock.IsBlocked).IsTrue();
    }

    /// <summary>Fails the next metadata publish after it is armed.</summary>
    [ThreadSafe]
    private sealed class MetaFailureHooks : IFollowerLogFaultHooks
    {
        private volatile bool _armed;

        public void OnBeforeMemoryApply()
        {
        }

        public void OnCommitAdvanced()
        {
        }

        public void OnFlushed()
        {
        }

        public void OnFrameWritten()
        {
        }

        public void OnFramesScheduling()
        {
        }

        public void OnMetaPublishing()
        {
            if (_armed)
                throw new IOException("Injected commit write failure.");
        }

        public void OnSnapshotPublishing()
        {
        }

        /// <summary>Makes every metadata publish fail from now on.</summary>
        internal void Arm() => _armed = true;
    }
}
