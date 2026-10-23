using System;
using System.Diagnostics.Metrics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaApplyServiceTests;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The apply loop of a follower group tells the election driver whether committed entries are pending and cannot be applied.</summary>
public sealed class ReplicaApplyBlockTests : ServerUnitTestBase
{
    /// <summary>The event id of the passes a leadership skips.</summary>
    private const int PassSkippedEventId = 4047;

    /// <summary>The event id of a pass that failed with a fault it retries.</summary>
    private const int RetryEventId = 4029;

    /// <summary>The event id of a group a committed record stopped.</summary>
    private const int StoppedEventId = 4028;

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>A group is unassessed until its loop decides; a pass that applies everything assesses it as not blocked.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliedPassAssessesTheGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-assessed");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        var block = registry.StateFor("n2").ApplyBlock;
        using var service = CreateService(registry, appliers, new EventRecordingLogger());
        var before = block.IsAssessed;
        var applied = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            await applied.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
            appliers.For("n2").DriverLease.EndLeading();
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(before).IsFalse();
        _ = await Assert.That(block.IsAssessed).IsTrue();
        _ = await Assert.That(block.IsBlocked).IsFalse();
    }

    /// <summary>A fault of the journal leaves the group blocked while its entry is pending, and the pass that applies it clears the block.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FaultBlocksUntilTheEntriesAreApplied(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-fault");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var writes = 0;
        var applied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, _, _, _) =>
                  {
                      var write = Interlocked.Increment(ref writes);
                      if (write == 1)
                          return ValueTask.FromException(new JournalCapacityExceededException());

                      if (write == 3)
                          _ = applied.TrySetResult();

                      return ValueTask.CompletedTask;
                  });
        var events = new EventRecordingLogger();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache.Instance(), meter);
        var block = registry.StateFor("n2").ApplyBlock;
        using var service = CreateService(registry, appliers, events);
        var before = block.IsBlocked;

        await service.StartAsync(cancellationToken);
        bool whileFaulting;
        bool recovered;
        try
        {
            await events.WhenLoggedAsync(RetryEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            whileFaulting = block is { IsBlocked: true, IsAssessed: true };
            _ = await new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers()).AppendAsync("n2", Fingerprint, 1, Batch(Prepare("k2", 2UL), 1UL, 2UL), cancellationToken);
            await applied.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

            // The lease waits for the running pass, which clears the block before it ends.
            await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
            appliers.For("n2").DriverLease.EndLeading();
            recovered = block.IsBlocked;
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(before).IsFalse();
        _ = await Assert.That(whileFaulting).IsTrue();
        _ = await Assert.That(recovered).IsFalse();
        _ = await Assert.That(registry.StateFor("n3").ApplyBlock.IsBlocked).IsFalse();
    }

    /// <summary>A group a committer leads is not judged by the loop: a block left by an earlier pass is cleared when the loop skips its pass.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeadingGroupClearsAStaleBlock(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-leading");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, new StubCache(), meter);
        await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
        var block = registry.StateFor("n2").ApplyBlock;
        block.Block();
        var events = new EventRecordingLogger();
        using var service = CreateService(registry, appliers, events);

        await service.StartAsync(cancellationToken);
        try
        {
            await events.WhenLoggedAsync(PassSkippedEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(block.IsBlocked).IsFalse();
    }

    /// <summary>A group this node leads from the start runs no apply loop, and is assessed at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LedGroupIsAssessedWithoutALoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-led");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var cache = new StubCache();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache, meter);
        using var service = CreateService(registry, appliers, new EventRecordingLogger());
        var applied = WhenAppliedAsync(cache, 1);

        await service.StartAsync(cancellationToken);
        try
        {
            // The loops start in group order, so the loop of n2 applying means the service already passed over n1.
            await applied.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(registry.StateFor("n1").ApplyBlock.IsAssessed).IsTrue();
    }

    /// <summary>A pass the loop skips because a lease is held assesses the group, so a leadership or a transfer does not leave it unassessed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SkippedPassAssessesTheGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-skipped");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, new StubCache(), meter);
        await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
        var block = registry.StateFor("n2").ApplyBlock;
        var events = new EventRecordingLogger();
        using var service = CreateService(registry, appliers, events);

        await service.StartAsync(cancellationToken);
        try
        {
            await events.WhenLoggedAsync(PassSkippedEventId, "group n2 ").WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(block.IsAssessed).IsTrue();
        _ = await Assert.That(block.IsBlocked).IsFalse();
    }

    /// <summary>A group a committed record stopped stays blocked and runs no further pass, until a state transfer reseeds it; the next pass then clears the block.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StoppedGroupResumesAfterReseed(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-apply-block-stopped");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await SeedAsync(registry, "n2", 1UL, cancellationToken, "k1");
        var writes = 0;
        var reapplied = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cache = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = cache.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback((_, _, _, _, _) =>
                  {
                      if (Interlocked.Increment(ref writes) == 1)
                          return ValueTask.FromException(new InvalidDataException("undecodable record"));

                      _ = reapplied.TrySetResult();
                      return ValueTask.CompletedTask;
                  });
        var events = new EventRecordingLogger();
        using var meter = new Meter("test");
        var appliers = CreateAppliers(registry, cache.Instance(), meter);
        var block = registry.StateFor("n2").ApplyBlock;
        using var service = CreateService(registry, appliers, events);

        await service.StartAsync(cancellationToken);
        bool stoppedBlocked;
        bool resumedBlocked;
        try
        {
            await events.WhenLoggedAsync(StoppedEventId).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            stoppedBlocked = block.IsBlocked;

            // A state transfer installs a clean copy: it resets the applied index under the lease and wakes the loop.
            appliers.For("n2").ResetTo(0UL);
            registry.ApplySignalFor("n2").Notify();
            await reapplied.Task.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
            await appliers.For("n2").DriverLease.LeadAsync(cancellationToken);
            appliers.For("n2").DriverLease.EndLeading();
            resumedBlocked = block.IsBlocked;
        }
        finally
        {
            await service.StopAsync(cancellationToken);
        }

        _ = await Assert.That(stoppedBlocked).IsTrue();
        _ = await Assert.That(resumedBlocked).IsFalse();
        _ = await Assert.That(events.Count(StoppedEventId)).IsEqualTo(1);
    }

    private static ReplicaApplyService CreateService(ReplicaGroupRegistry registry, ReplicaGroupAppliers appliers, EventRecordingLogger events) => new(
        registry,
        appliers,
        LeadOwnGroup(registry, appliers),
        ReplicaCommitterDoubles.RecoveryLifecycle.Recovered(),
        events,
        new FakeTimeProvider());
}
