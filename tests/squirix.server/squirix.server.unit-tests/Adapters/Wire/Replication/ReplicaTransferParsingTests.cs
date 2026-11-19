using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Adapters.Wire.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Pooled transfer parsing preserves protobuf fields while borrowing one owned message buffer.</summary>
public sealed class ReplicaTransferParsingTests
{
    /// <summary>Entry bytes share the rented message buffer and are returned exactly once after processing.</summary>
    [Test]
    public async Task EntryBytesBorrowMessageBuffer()
    {
        var request = new TransferReplicaStateRequest
        {
            Header = new ReplicationEnvelopeHeader { GroupId = "group", LeaderNodeId = "leader", Term = 2UL },
            TransferId = "transfer",
            Sequence = 3U,
            Entries = new ReplicaTransferEntries(),
        };
        request.Entries.Entries.Add(new ReplicaTransferEntry { CacheName = "cache", Key = ByteString.CopyFromUtf8("key"), Payload = ByteString.CopyFromUtf8("payload"), ExpiresUtcTicks = 42L });
        var bytes = request.ToByteArray();
        var parsed = ReplicaTransferParser.Parse(bytes, bytes.Length);
        var payload = parsed.Entries.Entries[0].Payload.Memory;

        _ = await Assert.That(parsed.Equals(request)).IsTrue();
        _ = await Assert.That(MemoryMarshal.TryGetArray(payload, out var segment) && ReferenceEquals(segment.Array, bytes)).IsTrue();
    }
}
