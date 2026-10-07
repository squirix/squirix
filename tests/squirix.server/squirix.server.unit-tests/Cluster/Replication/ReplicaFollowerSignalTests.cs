using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>A change the follower path accepts wakes the apply loop of its group; a refused one does not.</summary>
[Immutable]
public sealed class ReplicaFollowerSignalTests : ServerUnitTestBase
{
    private const string GroupId = "n2";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>An accepted append raises the apply signal of its group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedAppendRaisesSignal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-append");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry);

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1, Batch(1, 0, 0), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsTrue();
    }

    /// <summary>An accepted commit advance raises the apply signal of its group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitAdvanceRaisesSignal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-commit");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry);
        _ = await follower.AppendAsync(GroupId, Fingerprint, 1, Batch(1, 0, 0), cancellationToken);
        _ = await IsRaisedAsync(registry, cancellationToken);

        var result = await follower.AdvanceCommitAsync(GroupId, Fingerprint, 1, 1, 1, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsTrue();
    }

    /// <summary>An installed snapshot raises the apply signal of its group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotInstallRaisesSignal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-install");
        using var sourceDir = new TempDirectory("squirix-follower-signal-install-source");
        var snapshot = await PublishSnapshotAsync(sourceDir, cancellationToken);
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry);

        var result = await follower.InstallSnapshotAsync(GroupId, Fingerprint, 1, snapshot, 1UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue().Because($"refused with '{result.RefusalCode}'");
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsTrue();
    }

    /// <summary>An append the log refuses leaves the apply signal idle.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedAppendLeavesSignalIdle(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-refused");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry);

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1, Batch(2, 1, 0), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsFalse();
    }

    private static async Task<ReplicaGroupRegistry> OpenAsync(string dir, CancellationToken cancellationToken)
    {
        var registry = new ReplicaGroupRegistry(dir, ["n1", GroupId], 3, Fingerprint, 1, NullLoggerFactory.Instance);
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    /// <summary>Commits one entry on a source log of the group and compacts it, publishing a snapshot through index 1.</summary>
    /// <param name="dir">The source node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The published snapshot.</returns>
    /// <exception cref="InvalidOperationException">The source log refused a step or published no snapshot.</exception>
    private static async Task<GroupSnapshot> PublishSnapshotAsync(string dir, CancellationToken cancellationToken)
    {
        await using var source = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await source.OpenAsync(cancellationToken);
        await source.AdoptTopologyAsync(Fingerprint, 1, cancellationToken);
        FollowerLogEntry[] entries = [new(1UL, 1UL, new byte[] { 1 })];
        var appended = await source.AppendAsync(new FollowerLogAppendRequest("n2", 1UL, 0UL, 0UL, 1UL, entries), cancellationToken);
        var applied = await source.AdvanceAppliedAsync(1UL, cancellationToken);
        var compacted = await source.CompactThroughAsync(1UL, cancellationToken);
        var published = appended.Success && applied.Success && compacted == GroupCompactionOutcome.Compacted
            ? await new GroupSnapshotStore(dir, GroupId).ReadPublishedAsync(cancellationToken) : null;
        return published ?? NoSnapshot($"append '{appended.RefusalCode}', apply '{applied.RefusalCode}', compaction '{compacted}'");
    }

    /// <summary>Fails the snapshot setup.</summary>
    /// <param name="steps">The outcome of each setup step.</param>
    /// <returns>Never returns.</returns>
    /// <exception cref="InvalidOperationException">Always.</exception>
    private static GroupSnapshot NoSnapshot(string steps) => throw new InvalidOperationException($"The source log published no snapshot: {steps}.");

    /// <summary>Takes the pending notification of the group without waiting.</summary>
    /// <param name="registry">The registry of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns><see langword="true" /> when a notification was pending.</returns>
    private static Task<bool> IsRaisedAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        registry.ApplySignalFor(GroupId).WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

    private static FollowerBatch Batch(ulong logIndex, ulong prevIndex, ulong commitIndex)
    {
        var record = new ReplicaLogRecord(
            logIndex,
            1,
            $"op-{logIndex}",
            "cache",
            new byte[] { 1 },
            "UserMutation",
            "cache",
            Encoding.UTF8.GetBytes("k"),
            "Set",
            Encoding.UTF8.GetBytes("v"),
            ReadOnlyMemory<byte>.Empty,
            0,
            0,
            0,
            0);
        return new FollowerBatch([record], "n2", 1, prevIndex, prevIndex == 0 ? 0UL : 1UL, commitIndex);
    }
}
