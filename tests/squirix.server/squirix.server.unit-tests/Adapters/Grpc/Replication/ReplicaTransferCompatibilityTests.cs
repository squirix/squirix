using System;
using System.Buffers;
using System.IO;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Grpc.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Checks borrowed parsing against the generated protobuf parser.</summary>
public sealed class ReplicaTransferCompatibilityTests
{
    /// <summary>Every step retains its complete wire representation.</summary>
    [Test]
    public async Task EveryStepMatchesGeneratedParser()
    {
        TransferReplicaStateRequest[] requests =
        [
            new() { Begin = new ReplicaTransferBegin { Index = ulong.MaxValue, IndexTerm = 2, SnapshotBytes = 99, SnapshotChecksum = 17, CeilingChecked = true } },
            new() { Snapshot = new ReplicaTransferSnapshot { Offset = ulong.MaxValue, Chunk = ByteString.CopyFromUtf8("snapshot") } },
            new() { Wipe = new ReplicaTransferWipe() },
            new() { Entries = new ReplicaTransferEntries { Entries = { new ReplicaTransferEntry { Key = ByteString.CopyFromUtf8("k"), Payload = ByteString.CopyFromUtf8("value"), ExpiresUtcTicks = -1 } } } },
            new() { End = new ReplicaTransferEnd { EntryCount = ulong.MaxValue, EntriesCrc32C = uint.MaxValue } },
            new() { Abort = new ReplicaTransferAbort() },
        ];
        foreach (var request in requests)
        {
            request.Header = new ReplicationEnvelopeHeader { SchemaVersion = 1, SenderNodeId = "sender", TopologyFingerprint = ByteString.CopyFromUtf8("fingerprint") };
            request.TransferId = "transfer";
            request.Sequence = uint.MaxValue;
            using var parsed = ReplicaTransferWire.Parse(new ReadOnlySequence<byte>(request.ToByteArray()), ArrayPool<byte>.Shared);
            _ = await Assert.That(parsed.Request.Equals(request)).IsTrue();
        }
    }

    /// <summary>Repeated message fields merge, oneof changes reset the previous step, and unknown fields survive.</summary>
    [Test]
    public async Task MergedFieldsMatchGeneratedParser()
    {
        byte[][] parts =
        [
            new TransferReplicaStateRequest { Header = new ReplicationEnvelopeHeader { GroupId = "g" }, Entries = new ReplicaTransferEntries { Entries = { new ReplicaTransferEntry { CacheName = "a" } } } }.ToByteArray(),
            new TransferReplicaStateRequest { Header = new ReplicationEnvelopeHeader { Term = 8 }, Entries = new ReplicaTransferEntries { Entries = { new ReplicaTransferEntry { CacheName = "b" } } } }.ToByteArray(),
            new TransferReplicaStateRequest { Snapshot = new ReplicaTransferSnapshot { Offset = 9, Chunk = ByteString.CopyFromUtf8("old") } }.ToByteArray(),
            [0x2A, 0x02, 0x10, 0x00],
            [0x78, 0x01, 0x83, 0x01, 0x08, 0x01, 0x84, 0x01],
            [0x3A, 0x0A, 0x0A, 0x08, 0x12, 0x00, 0x1A, 0x00, 0x38, 0x01, 0x08, 0x02],
        ];
        byte[] bytes = [];
        foreach (var part in parts)
        {
            bytes = [.. bytes, .. part];
            var expected = TransferReplicaStateRequest.Parser.ParseFrom(bytes);
            using var actual = ReplicaTransferWire.Parse(new ReadOnlySequence<byte>(bytes), ArrayPool<byte>.Shared);
            _ = await Assert.That(actual.Request.Equals(expected)).IsTrue();
        }
    }

    /// <summary>Truncation, invalid tags, lengths and varints have the same acceptance as generated protobuf.</summary>
    [Test]
    public async Task MalformedMessagesMatchGeneratedParser()
    {
        foreach (var bytes in new byte[][] { [], [0], [0x3A, 5, 10], [0x3A, 2, 10, 127], [0x2A, 3, 18, 127, 0], [12], [0x78, 0x80], [0x18, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 1] })
            _ = await Assert.That(BorrowedResult(bytes)).IsEqualTo(GeneratedResult(bytes));

        for (var i = 0; i < 2000; i++)
        {
            var bytes = SHA256.HashData(BitConverter.GetBytes(i));
            _ = await Assert.That(BorrowedResult(bytes)).IsEqualTo(GeneratedResult(bytes));
        }
    }

    /// <summary>Unknown groups obey protobuf recursion limits at each borrowed message depth.</summary>
    [Test]
    public async Task NestedGroupsMatchGeneratedParser()
    {
        for (var depth = 98; depth <= 102; depth++)
        {
            var groups = new byte[depth * 2];
            groups.AsSpan(0, depth).Fill(0x7B);
            groups.AsSpan(depth).Fill(0x7C);
            foreach (var bytes in new[] { groups, Wrap(groups, 0x2A), Wrap(groups, 0x3A), Wrap(Wrap(groups, 0x0A), 0x3A) })
                _ = await Assert.That(BorrowedResult(bytes)).IsEqualTo(GeneratedResult(bytes));
        }
    }

    private static string? BorrowedResult(byte[] bytes)
    {
        try
        {
            using var buffer = ReplicaTransferWire.Parse(new ReadOnlySequence<byte>(bytes), ArrayPool<byte>.Shared);
            return Convert.ToBase64String(buffer.Request.ToByteArray());
        }
        catch (InvalidDataException)
        {
            return null;
        }
    }

    private static string? GeneratedResult(byte[] bytes)
    {
        try
        {
            return Convert.ToBase64String(TransferReplicaStateRequest.Parser.ParseFrom(bytes).ToByteArray());
        }
        catch (InvalidProtocolBufferException)
        {
            return null;
        }
    }

    private static byte[] Wrap(byte[] bytes, byte tag)
    {
        var wrapped = new byte[1 + CodedOutputStream.ComputeBytesSize(UnsafeByteOperations.UnsafeWrap(bytes))];
        using var output = new CodedOutputStream(wrapped);
        output.WriteRawTag(tag);
        output.WriteBytes(UnsafeByteOperations.UnsafeWrap(bytes));
        return wrapped;
    }
}
