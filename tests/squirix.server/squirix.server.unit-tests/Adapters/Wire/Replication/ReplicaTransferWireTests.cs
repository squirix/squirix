using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using ReplicaStateEntry = Squirix.Server.Cluster.Replication.ReplicaStateEntry;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Checks the native state transfer wire format.</summary>
public sealed class ReplicaTransferWireTests
{
    /// <summary>Every step retains its complete wire representation.</summary>
    [Test]
    public async Task EachTransferStepRoundTrips()
    {
        TransferReplicaStateRequest[] requests =
        [
            new() { Begin = new ReplicaTransferBegin { Index = ulong.MaxValue, IndexTerm = 2, SnapshotBytes = 99, SnapshotChecksum = 17, CeilingChecked = true } },
            new() { Snapshot = new ReplicaTransferSnapshot { Offset = ulong.MaxValue, Chunk = Encoding.UTF8.GetBytes("snapshot") } },
            new() { Wipe = new ReplicaTransferWipe() },
            new() { Entries = new ReplicaTransferEntries { Entries = new ReplicaStateEntry[] { new(string.Empty, Encoding.UTF8.GetBytes("k"), Encoding.UTF8.GetBytes("value"), -1) } } },
            new() { End = new ReplicaTransferEnd { EntryCount = ulong.MaxValue, EntriesCrc32C = uint.MaxValue } },
            new() { Abort = new ReplicaTransferAbort() },
        ];
        foreach (var request in requests)
        {
            request.Header = new EnvelopeHeader { SchemaVersion = 1, SenderNodeId = "sender", TopologyFingerprint = Encoding.UTF8.GetBytes("fingerprint") };
            request.TransferId = "transfer";
            request.Sequence = uint.MaxValue;
            var bytes = request.ToByteArray();
            var parsed = TransferReplicaStateRequest.Parser.ParseBorrowed(bytes);
            _ = await Assert.That(parsed.StepCase).IsEqualTo(request.StepCase);
            _ = await Assert.That(parsed.ToByteArray().AsSpan().SequenceEqual(bytes)).IsTrue();
        }
    }

    /// <summary>Homogeneous transfers send one name, while mixed transfers preserve names and order.</summary>
    /// <param name="shared">Whether both entries belong to the same cache.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task EntryBatchRetainsNamesAndPayloads(bool shared)
    {
        ReplicaStateEntry[] entries = [new("a", new byte[] { 1 }, new byte[] { 3 }, -1), new(shared ? "a" : "b", new byte[] { 2 }, default, 1)];
        var batch = new ReplicaTransferEntries { Entries = entries };
        byte[] expected = shared ? [9, 1, 97, 1, 1, 1, 3, 1, 1, 2, 0, 2] : [8, 1, 97, 1, 1, 1, 3, 1, 1, 98, 1, 2, 0, 2];
        _ = await Assert.That(batch.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        var parsed = ReplicaTransferEntries.Parser.ParseBorrowed(expected);
        _ = await Assert.That(parsed.Entries.Span[0].CacheName).IsEqualTo("a");
        _ = await Assert.That(parsed.Entries.Span[1].CacheName).IsEqualTo(shared ? "a" : "b");
        _ = await Assert.That(parsed.Entries.Span[0].Payload.Span[0]).IsEqualTo(byte.CreateChecked(3));
        _ = await Assert.That(parsed.Entries.Span[1].Key.Span[0]).IsEqualTo(byte.CreateChecked(2));
        _ = await Assert.That(parsed.Entries.Span[0].ExpiresUtcTicks).IsEqualTo(-1L);
        _ = await Assert.That(parsed.Entries.Span[1].ExpiresUtcTicks).IsEqualTo(1L);
    }

    /// <summary>A batch of nonexpiring entries omits every deadline and restores zero on decode.</summary>
    [Test]
    public async Task OmitsAbsentDeadlines()
    {
        ReplicaStateEntry[] entries = [new("a", default, default, 0), new("a", default, default, 0)];
        byte[] expected = [11, 1, 97, 0, 0, 0, 0];
        _ = await Assert.That(new ReplicaTransferEntries { Entries = entries }.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        var parsed = ReplicaTransferEntries.Parser.ParseBorrowed(expected);
        _ = await Assert.That((parsed.Entries.Span[0].ExpiresUtcTicks, parsed.Entries.Span[1].ExpiresUtcTicks)).IsEqualTo((0L, 0L));
    }

    /// <summary>The version bit preserves both identity claims and an independent group identifier.</summary>
    /// <param name="leader">The claimed leader.</param>
    /// <param name="length">The expected size with both identities restored.</param>
    [Test]
    [Arguments("node-a", 13)]
    [Arguments("node-b", 20)]
    public async Task EncodesIndependentIdentityClaims(string leader, int length)
    {
        var header = new EnvelopeHeader { SchemaVersion = 1, GroupId = "g", SenderNodeId = "node-a", LeaderNodeId = leader };
        var bytes = header.ToByteArray();
        _ = await Assert.That(bytes.Length).IsEqualTo(length);
        var parsed = EnvelopeHeader.Parser.ParseFrom(bytes);
        _ = await Assert.That((parsed.GroupId, parsed.SenderNodeId, parsed.LeaderNodeId, parsed.SchemaVersion)).IsEqualTo(("g", "node-a", leader, 1U));
    }

    /// <summary>A generated transfer ID occupies sixteen raw bytes and one selector.</summary>
    [Test]
    public async Task EncodesCanonicalTransferId()
    {
        const string id = "00112233445566778899aabbccddeeff";
        var request = new TransferReplicaStateRequest { TransferId = id, Wipe = new ReplicaTransferWipe() };
        var bytes = request.ToByteArray();
        byte[] expected = [0, 0, .. Convert.FromHexString(id), 0, 3];
        _ = await Assert.That(bytes.AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(TransferReplicaStateRequest.Parser.ParseFrom(bytes).TransferId).IsEqualTo(id);
    }

    /// <summary>A shared name requires at least two entries and complete entry fields.</summary>
    [Test]
    public void RejectsInvalidSharedNameBatch()
    {
        foreach (var frame in new byte[][] { [1], [3, 0], [9, 1, 97, 0, 0, 0], [11, 1, 97, 0, 0, 0], [255, 255, 255, 127] })
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(frame, static bytes => _ = ReplicaTransferEntries.Parser.ParseBorrowed(bytes));
    }

    /// <summary>Both owned and borrowed parsing reject a short snapshot and trailing bytes.</summary>
    [Test]
    public void TransferPayloadMustBeComplete()
    {
        var request = new TransferReplicaStateRequest { Snapshot = new ReplicaTransferSnapshot { Offset = 9, Chunk = Encoding.UTF8.GetBytes("snapshot") } };
        var bytes = request.ToByteArray();
        foreach (var length in new[] { 0, bytes.Length - 1 })
        {
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws((bytes, length), static state => _ = TransferReplicaStateRequest.Parser.ParseFrom(state.bytes, 0, state.length));
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes.AsMemory(0, length), static state => _ = TransferReplicaStateRequest.Parser.ParseBorrowed(state));
        }

        byte[] trailing = [.. bytes, 0];
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(trailing, static state => _ = TransferReplicaStateRequest.Parser.ParseBorrowed(state));
    }
}
