using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Grpc.Replication.SquirixReplicationFollowerAdapterTests;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Unit tests for the canonical entries of an append request at <see cref="SquirixReplicationServiceAdapter" />.</summary>
[Immutable]
public sealed class SquirixReplicationAppendAdapterTests : ServerUnitTestBase
{
    /// <summary>Verifies that the canonical bytes of a wire entry land in the log unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WireEntryBytesAreStoredAsReceivedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var payload = CanonicalPayload(1, 7);
        var request = AppendRequest(follower, 1, 7, payload);

        var response = await follower.Adapter.AppendReplicaEntries(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That((response.Success, response.LastLogIndex)).IsEqualTo((true, 1UL));
        var tail = await GroupLog(follower).GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(1);
        _ = await Assert.That((tail[0].LogIndex, tail[0].Term)).IsEqualTo((1UL, 7UL));
        _ = await Assert.That(tail[0].Payload.Span.SequenceEqual(payload)).IsTrue();
    }

    /// <summary>Verifies that a wire entry that is not a canonical record at its position is refused with a checksum mismatch and stores nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MalformedWireEntryIsRefusedAsync(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var request = AppendRequest(follower, 1, 7, CanonicalPayload(2, 7));

        var response = await follower.Adapter.AppendReplicaEntries(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That((response.Success, response.RefusalCode, response.Term, response.LastLogIndex)).IsEqualTo((false, RefusalCodes.ChecksumMismatch, 0UL, 0UL));
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogIndex)).IsEqualTo((0UL, 0UL));
    }

    private static byte[] CanonicalPayload(ulong logIndex, ulong term)
    {
        var record = new ReplicaLogRecord(logIndex, term, "op", "cache", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 1 }, "Set", new byte[] { 2 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0);
        return ReplicaLogCodec.Encode(in record);
    }

    private static AppendReplicaEntriesRequest AppendRequest(FollowerScope follower, ulong logIndex, ulong term, byte[] payload)
    {
        var request = new AppendReplicaEntriesRequest { Header = follower.Header, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommitIndex = 0 };
        request.LogEntries.Add(new ReplicaLogEntry { LogIndex = logIndex, Term = term, CanonicalPayload = ByteString.CopyFrom(payload) });
        return request;
    }
}
