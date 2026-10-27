using System;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Entry catch-up from a leader log entry that is not a canonical record at its own position.</summary>
[Immutable]
public sealed class ReplicaEntryCatchUpCorruptTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A leader entry whose payload is not a canonical record ends the session as corrupt, before anything is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task UndecodableLeaderEntryIsCorrupt(CancellationToken cancellationToken) =>
        AssertCorruptAsync(new FollowerLogEntry(1, 1, new byte[] { 1, 2, 3 }), cancellationToken);

    /// <summary>A leader entry whose payload carries another position than the entry ends the session as corrupt, before anything is sent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public Task MisplacedLeaderEntryIsCorrupt(CancellationToken cancellationToken)
    {
        var record = new ReplicaLogRecord(2, 1, "op", "cache", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 1 }, "Set", new byte[] { 1 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0, 0);

        return AssertCorruptAsync(new FollowerLogEntry(1, 1, ReplicaLogCodec.Encode(in record)), cancellationToken);
    }

    private static async Task AssertCorruptAsync(FollowerLogEntry entry, CancellationToken cancellationToken)
    {
        var status = new FollowerLogStatus(GroupId, Fingerprint, 1, 1, string.Empty, 3, 1, 3, 0, FollowerLogReadiness.Ready, GroupMemberState.Complete);
        var expectations = new IFollowerLogCreateExpectations();
        _ = expectations.Setups.GetStatusAsync(Arg.Any<CancellationToken>()).ReturnValue(ValueTask.FromResult(status));
        _ = expectations.Setups.ReadEntriesAsync(Arg.Any<ulong>(), Arg.Any<int>(), Arg.Any<CancellationToken>())
                        .ReturnValue(Task.FromResult(new FollowerLogEntriesRead(true, 0, 0, 3, 0, [entry])));
        var gateway = new ParkingFollowerGateway();
        var sender = CreateSender(gateway);
        try
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);

            var result = await new ReplicaEntryCatchUpSession(expectations.Instance(), "n1", 1).RunAsync(lease, cancellationToken);

            _ = await Assert.That(result).IsEqualTo(new ReplicaCatchUpResult(ReplicaCatchUpOutcome.Corrupt, 0, 0, 0, 0, 0));
            _ = await Assert.That(gateway.CallCount).IsEqualTo(0);
        }
        finally
        {
            await sender.DisposeAsync();
        }
    }
}
