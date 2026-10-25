using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>Shared checks and policies of the tests that compact the group log of the RF=3 owner harness.</summary>
internal static class OwnedLogTestKit
{
    /// <summary>The zero-based slot of node n3, the follower the scripted gateway silences.</summary>
    internal const int FollowerSlot = 2;

    /// <summary>Compacts at any entry, and every log is past its retention limit.</summary>
    internal static readonly ReplicaLogCompactionPolicy PastLimit = new(long.MaxValue, 1, 0L);

    internal static async Task AssertNotCompactedAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var log = OwnedLog(registry);
        var retention = await log.GetRetentionAsync(cancellationToken);
        _ = await Assert.That(retention.SnapshotIndex).IsEqualTo(0UL);
        _ = await Assert.That(retention.RetainedEntries > 0).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    internal static IJournalDurabilityCoordinator Durable()
    {
        var durability = new IJournalDurabilityCoordinatorCreateExpectations();
        _ = durability.Setups.AwaitDurabilityCommitAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.CompletedTask);
        return durability.Instance();
    }

    internal static long HeaderLength() => GroupLogSlots.HeaderSize;

    internal static IFollowerLog OwnedLog(ReplicaGroupRegistry registry) =>
        registry.TryGetLog("n1", out var log) ? log : throw new InvalidOperationException("The owned group log is not open.");

    internal static async Task WriteAsync(ReplicaGroupCommitter committer, int count, CancellationToken cancellationToken)
    {
        for (var i = 0; i < count; i++)
        {
            var key = NewOperationId();
            await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken);
        }
    }
}
