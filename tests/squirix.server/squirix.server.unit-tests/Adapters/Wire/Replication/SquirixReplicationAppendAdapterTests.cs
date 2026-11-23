using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Adapters.Wire.Replication.SquirixReplicationFollowerAdapterTests;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

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

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, follower.Context);

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

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, follower.Context);

        _ = await Assert.That((response.Success, response.RefusalCode, response.Term, response.LastLogIndex)).IsEqualTo((false, RefusalCodes.ChecksumMismatch, 0UL, 0UL));
        var status = await GroupStatusAsync(follower, cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.LastLogIndex)).IsEqualTo((0UL, 0UL));
    }

    /// <summary>Parsed binary payloads outlive overwritten RPC input and are retained without a second payload copy.</summary>
    /// <param name="offset">The starting offset of the frame in the transport buffer.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(5)]
    public async Task ParsedPayloadIsRetainedWithoutCopy(int offset, CancellationToken cancellationToken)
    {
        await using var follower = await CreateFollowerScopeAsync("node-a", cancellationToken);
        var firstPayload = CanonicalPayload(1, 6);
        var secondPayload = CanonicalPayload(2, 7);
        var original = AppendRequest(follower, 1, 6, firstPayload);
        original.Entries = [.. original.Entries, new(2, 7, secondPayload)];
        var length = original.CalculateSize();
        var wire = new byte[offset + length + 3];
        original.WriteTo(wire.AsSpan(offset, length));
        var request = AppendReplicaEntriesRequest.Parser.ParseFrom(wire, offset, length);
        var firstMemory = request.Entries[0].Payload;
        var secondMemory = request.Entries[1].Payload;
        Array.Fill<byte>(wire, 255);

        var response = await follower.Adapter.AppendReplicaEntriesAsync(request, follower.Context);
        request.Entries = [];

        _ = await Assert.That((response.Success, response.LastLogIndex)).IsEqualTo((true, 2UL));
        var tail = await GroupLog(follower).GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That((tail[0].Term, tail[1].Term)).IsEqualTo((6UL, 7UL));
        _ = await Assert.That(tail[0].Payload.Equals(firstMemory)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Equals(secondMemory)).IsTrue();
        _ = await Assert.That(tail[0].Payload.Span.SequenceEqual(firstPayload)).IsTrue();
        _ = await Assert.That(tail[1].Payload.Span.SequenceEqual(secondPayload)).IsTrue();
    }

    private static byte[] CanonicalPayload(ulong logIndex, ulong term)
    {
        var record = new ReplicaLogRecord(logIndex, term, "op", "cache", new byte[] { 1 }, "user-mutation", "cache", new byte[] { 1 }, "set", new byte[] { 2 }, ReadOnlyMemory<byte>.Empty, 0, 0, 0);
        return ReplicaLogCodec.Encode(in record);
    }

    private static AppendReplicaEntriesRequest AppendRequest(FollowerScope follower, ulong logIndex, ulong term, byte[] payload) =>
        new() { Header = follower.Header, Entries = [new(logIndex, term, payload)] };
}
