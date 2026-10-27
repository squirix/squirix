using System;
using System.Collections.Concurrent;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Cluster.Replication.ReplicaSenderTestKit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Entry catch-up against a follower whose copy is not complete: the session ends at once, with an outcome of its own for each state.</summary>
[Immutable]
public sealed class ReplicaEntryCatchUpNotCompleteTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>A follower that answers incomplete, or unseeded, ends the session with the outcome of that state after one request.</summary>
    /// <param name="incomplete">Whether the follower copy is planted incomplete; otherwise it is unseeded.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task NotCompleteFollowerEndsTheSession(bool incomplete, CancellationToken cancellationToken)
    {
        var state = incomplete ? GroupMemberState.Incomplete : GroupMemberState.Unseeded;
        var outcome = incomplete ? ReplicaCatchUpOutcome.Incomplete : ReplicaCatchUpOutcome.Unseeded;
        var code = incomplete ? RefusalCodes.Incomplete : RefusalCodes.Unseeded;
        using var dir = new TempDirectory("squirix-catch-up-not-complete");
        await using var leader = new FollowerLog(Path.Join(dir, "leader"), GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await leader.OpenSeededAsync(cancellationToken);
        await AppendOneAsync(leader, cancellationToken);
        var followerDir = Path.Join(dir, "follower");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(followerDir, GroupId));
        var meta = new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 0UL, string.Empty, 0UL, 0UL, state, false);
        await GroupMetadataFile.WriteAsync(GroupStoragePaths.GetMetadataPath(followerDir, GroupId), in meta, cancellationToken);
        await using var follower = new FollowerLog(followerDir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await follower.OpenAsync(cancellationToken);
        var gateway = new FollowerLogRoutingGateway(new ConcurrentDictionary<string, FollowerLog>(StringComparer.Ordinal) { ["n2"] = follower });
        var status = await leader.GetStatusAsync(cancellationToken);
        var header = new ReplicaRpcHeader(GroupId, Fingerprint, 1, 1, "n1", "n1");

        ReplicaCatchUpResult result;
        var sender = new ReplicaFollowerSender(gateway, "n2", in header, status.LastLogIndex, status.LastLogTerm, HangGuard) { TimeProvider = TimeProvider.System };
        await using (sender)
        {
            using var lease = await sender.BeginCatchUpAsync(cancellationToken);
            var session = new ReplicaEntryCatchUpSession(leader, "n1", 1);
            result = await BoundedAsync(session.RunAsync(lease, cancellationToken), cancellationToken);
        }

        _ = await Assert.That((result.Outcome, result.Rounds, result.HeldThrough)).IsEqualTo((outcome, 1, 0UL));
        _ = await Assert.That(gateway.Refusals.TryPeek(out var refusal) && string.Equals(refusal.Refusal, code, StringComparison.Ordinal)).IsTrue();
    }

    private static async Task AppendOneAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var record = new ReplicaLogRecord(
            1,
            1,
            "op-1",
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
            0);
        var entry = new FollowerLogEntry(1, 1, ReplicaLogCodec.Encode(in record));
        var request = new FollowerLogAppendRequest("n1", 1, 0, 0, 0, new ReadOnlyMemory<FollowerLogEntry>([entry]));
        _ = await Assert.That((await log.AppendAsync(request, cancellationToken)).Success).IsTrue();
    }
}
