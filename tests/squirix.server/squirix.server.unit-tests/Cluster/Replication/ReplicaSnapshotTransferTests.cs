using System;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Snapshot transfer bound enforcement.</summary>
[Immutable]
public sealed class ReplicaSnapshotTransferTests : ServerUnitTestBase
{
    private const string GroupId = "transfer-group";

    /// <summary>Later caller buffer mutations cannot alter the transfer checksum-protected content.</summary>
    [Test]
    public async Task CreateDetachesPayloadBuffers()
    {
        var fingerprint = new byte[] { 1 };
        var outcome = new byte[] { 2 };
        var snapshot = Snapshot(fingerprint, outcome);
        var transfer = ReplicaSnapshotTransfer.Create(in snapshot);

        fingerprint[0] = 9;
        outcome[0] = 9;

        _ = await Assert.That(transfer.IsValidFor(GroupId)).IsTrue();
    }

    /// <summary>A payload above the configured bound fails validation instead of renting.</summary>
    [Test]
    public async Task OversizedPayloadFailsValidation()
    {
        var snapshot = Snapshot();
        var transfer = ReplicaSnapshotTransfer.Create(in snapshot);

        _ = await Assert.That(transfer.IsValidFor(GroupId)).IsTrue();
        _ = await Assert.That(transfer.IsValidFor(GroupId, transfer.PayloadLength - 1)).IsFalse();
    }

    /// <summary>Creating a transfer above the configured bound throws before renting.</summary>
    [Test]
    public void OversizedPayloadRejectsCreation()
    {
        var snapshot = Snapshot();

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(
            (Snapshot: snapshot, MaxBytes: 100),
            static state => _ = ReplicaSnapshotTransfer.Create(in state.Snapshot, state.MaxBytes));
    }

    private static GroupSnapshot Snapshot() => Snapshot([4, 8, 15], [2]);

    private static GroupSnapshot Snapshot(byte[] fingerprint, byte[] outcome) => new(
        GroupId,
        fingerprint,
        1UL,
        1UL,
        2UL,
        2UL,
        [
            new GroupIdempotencyRecord(
                "scope",
                "op",
                fingerprint,
                outcome,
                GroupRecordKind.UserMutation,
                new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc),
                new DateTime(2026, 1, 1, 0, 0, 1, DateTimeKind.Utc),
                1UL,
                1UL),
        ]);
}
