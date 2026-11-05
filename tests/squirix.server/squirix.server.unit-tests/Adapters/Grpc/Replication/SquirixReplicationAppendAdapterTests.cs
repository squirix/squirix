using System;
using System.Buffers;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
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

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, new TestServerCallContext(null, follower.HttpContext));

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

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, new TestServerCallContext(null, follower.HttpContext));

        _ = await Assert.That((response.Success, response.RefusalCode, response.Term, response.LastLogIndex)).IsEqualTo((false, RefusalCodes.ChecksumMismatch, 0UL, 0UL));
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogIndex)).IsEqualTo((0UL, 0UL));
    }

    /// <summary>Parsed protobuf payloads outlive overwritten RPC input and are retained without a second payload copy.</summary>
    /// <param name="segmented">Whether parsing crosses multiple transport segments.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ParsedPayloadIsRetainedWithoutCopy(bool segmented, CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var firstPayload = CanonicalPayload(1, 6);
        var secondPayload = CanonicalPayload(2, 7);
        var original = AppendRequest(follower, 1, 6, firstPayload);
        original.Entries.Add(new ReplicaLogEntry { LogIndex = 2, Term = 7, CanonicalPayload = ByteString.CopyFrom(secondPayload) });
        var wire = original.ToByteArray();
        var payloadOffset = wire.AsSpan().IndexOf(firstPayload);
        _ = await Assert.That(payloadOffset).IsGreaterThan(0);
        var request = AppendReplicaEntriesRequest.Parser.ParseFrom(Sequence(wire, segmented, payloadOffset + (firstPayload.Length / 2)));
        var firstMemory = request.Entries[0].CanonicalPayload.Memory;
        var secondMemory = request.Entries[1].CanonicalPayload.Memory;
        Array.Fill<byte>(wire, 255);

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, new TestServerCallContext(null, follower.HttpContext));
        request.Entries[0].CanonicalPayload = ByteString.Empty;
        request.Entries.Clear();

        _ = await Assert.That((response.Success, response.LastLogIndex)).IsEqualTo((true, 2UL));
        var tail = await GroupLog(follower).GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That((tail[0].Term, tail[1].Term)).IsEqualTo((6UL, 7UL));
        _ = await Assert.That(tail[0].Payload.Equals(firstMemory)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Equals(secondMemory)).IsTrue();
        _ = await Assert.That(tail[0].Payload.Span.SequenceEqual(firstPayload)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Span.SequenceEqual(secondPayload)).IsTrue();
    }

    /// <summary>A domain batch without an ownership contract continues to isolate a mutable caller array.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DomainAppendStillCopiesMutableInput(CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var payload = CanonicalPayload(1, 7);
        var domain = new ReplicaFollower(follower.Registry, RocksDoubles.CreateReplicaMembers());
        var batch = new FollowerBatch([new FollowerLogEntry(1, 7, payload)], "node-a", 7, 0, 0, 0);
        var response = await domain.AppendAsync("node-a", follower.Header.TopologyFingerprint.Memory, follower.Header.ConfigurationGeneration, batch, cancellationToken);
        Array.Fill<byte>(payload, 255);

        _ = await Assert.That(response.Success).IsTrue();
        var tail = await GroupLog(follower).GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(1);
        _ = await Assert.That(tail[0].Payload.Equals(payload.AsMemory())).IsFalse();
        _ = await Assert.That(tail[0].Payload.Span.SequenceEqual(CanonicalPayload(1, 7))).IsTrue();
    }

    private static ReadOnlySequence<byte> Sequence(byte[] wire, bool segmented, int split)
    {
        if (!segmented)
            return new ReadOnlySequence<byte>(wire);

        var first = new PayloadSegment(wire.AsMemory(0, split));
        var last = first.Append(wire.AsMemory(split));
        return new ReadOnlySequence<byte>(first, 0, last, last.Memory.Length);
    }

    private static byte[] CanonicalPayload(ulong logIndex, ulong term)
    {
        var record = new ReplicaLogRecord(logIndex, term, "op", "cache", new byte[] { 1 }, "UserMutation", "cache", new byte[] { 1 }, "Set", new byte[] { 2 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0);
        return ReplicaLogCodec.Encode(in record);
    }

    private static AppendReplicaEntriesRequest AppendRequest(FollowerScope follower, ulong logIndex, ulong term, byte[] payload)
    {
        var request = new AppendReplicaEntriesRequest { Header = follower.Header, PrevLogIndex = 0, PrevLogTerm = 0, LeaderCommitIndex = 0 };
        request.Entries.Add(new ReplicaLogEntry { LogIndex = logIndex, Term = term, CanonicalPayload = ByteString.CopyFrom(payload) });
        return request;
    }

    [Mutable]
    private sealed class PayloadSegment : ReadOnlySequenceSegment<byte>
    {
        internal PayloadSegment(ReadOnlyMemory<byte> memory)
        {
            Memory = memory;
        }

        internal PayloadSegment Append(ReadOnlyMemory<byte> memory)
        {
            var next = new PayloadSegment(memory) { RunningIndex = RunningIndex + Memory.Length };
            Next = next;
            return next;
        }
    }
}
