using System;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.Cluster.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Pooled transfer parsing preserves transfer fields while borrowing one owned message buffer.</summary>
public sealed class ReplicaTransferParsingTests
{
    /// <summary>Entry bytes share the rented message buffer through processing.</summary>
    [Test]
    public async Task EntryBytesBorrowMessageBuffer()
    {
        var request = new TransferReplicaStateRequest
        {
            Header = new EnvelopeHeader { GroupId = "group", LeaderNodeId = "leader", Term = 2UL },
            TransferId = "transfer",
            Sequence = 3U,
            Entries = new ReplicaTransferEntries { Entries = new ReplicaStateEntry[] { new("cache", Encoding.UTF8.GetBytes("key"), Encoding.UTF8.GetBytes("payload"), 42L) } },
        };
        var bytes = request.ToByteArray();
        var parsed = TransferReplicaStateRequest.Parser.ParseBorrowed(bytes);
        var payload = parsed.Entries.Entries.Span[0].Payload;

        _ = await Assert.That(payload.Span.SequenceEqual("payload"u8)).IsTrue();
        _ = await Assert.That(MemoryMarshal.TryGetArray(payload, out var segment) && ReferenceEquals(segment.Array, bytes)).IsTrue();
    }
}
