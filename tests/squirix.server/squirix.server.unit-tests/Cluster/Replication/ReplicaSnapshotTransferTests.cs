using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Snapshot transfer bound enforcement.</summary>
[Immutable]
public sealed class ReplicaSnapshotTransferTests : ServerUnitTestBase
{
    private const string GroupId = "transfer-group";

    /// <summary>A payload above the configured bound fails validation instead of renting.</summary>
    [Fact]
    public void OversizedPayloadFailsValidation()
    {
        var snapshot = Snapshot();
        var transfer = ReplicaSnapshotTransfer.Create(in snapshot);

        Assert.True(transfer.IsValidFor(GroupId));
        Assert.False(transfer.IsValidFor(GroupId, transfer.PayloadLength - 1));
    }

    /// <summary>Creating a transfer above the configured bound throws before renting.</summary>
    [Fact]
    public void OversizedPayloadRejectsCreation()
    {
        var snapshot = Snapshot();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            (Snapshot: snapshot, MaxBytes: 100),
            static state => _ = ReplicaSnapshotTransfer.Create(in state.Snapshot, state.MaxBytes));
    }

    private static GroupSnapshot Snapshot() => new(
        GroupId,
        new byte[] { 4, 8, 15 },
        1UL,
        1UL,
        2UL,
        2UL,
        [
            new GroupIdempotencyRecord(
                "scope",
                "op",
                new byte[] { 1 },
                new byte[] { 2 },
                GroupRecordKind.UserMutation,
                new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 1, 1, 0, 0, 1, DateTimeKind.Utc),
                1UL,
                1UL),
        ]);
}
