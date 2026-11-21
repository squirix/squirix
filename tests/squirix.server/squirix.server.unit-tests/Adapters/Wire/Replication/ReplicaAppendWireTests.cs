using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Wire.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire.Replication;

/// <summary>Checks compact and explicit replica append batches.</summary>
public sealed class ReplicaAppendWireTests
{
    /// <summary>A contiguous run stores its term once and derives each index from the previous log position.</summary>
    [Test]
    public async Task ConsecutiveEntriesUseOneTerm()
    {
        var request = new AppendReplicaEntriesRequest { PrevLogIndex = 126, Entries = [new(127, 7, new byte[] { 170 }), new(128, 7, new byte[] { 187 })] };
        byte[] expected = [2, 126, 0, 0, 2, 7, 1, 170, 1, 187];
        _ = await Assert.That(request.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();

        var decoded = AppendReplicaEntriesRequest.Parser.ParseFrom(expected);
        _ = await Assert.That(decoded.Entries.Count).IsEqualTo(2);
        _ = await Assert.That(decoded.Entries[0].LogIndex).IsEqualTo(127UL);
        _ = await Assert.That(decoded.Entries[1].LogIndex).IsEqualTo(128UL);
        _ = await Assert.That(decoded.Entries[0].Term).IsEqualTo(7UL);
        _ = await Assert.That(decoded.Entries[1].Term).IsEqualTo(7UL);
        _ = await Assert.That(decoded.Entries[0].Payload.Span[0]).IsEqualTo(byte.CreateChecked(170));
        _ = await Assert.That(decoded.Entries[1].Payload.Span[0]).IsEqualTo(byte.CreateChecked(187));
    }

    /// <summary>Catch-up term changes and nonconsecutive indexes reach the receiver exactly as sent.</summary>
    /// <param name="secondIndex">The index of the second entry.</param>
    /// <param name="secondTerm">The term of the second entry.</param>
    [Test]
    [Arguments(11UL, 8UL)]
    [Arguments(12UL, 7UL)]
    public async Task IrregularEntriesRetainExplicitPositions(ulong secondIndex, ulong secondTerm)
    {
        var request = new AppendReplicaEntriesRequest { PrevLogIndex = 9, Entries = [new(10, 7, default), new(secondIndex, secondTerm, default)] };
        var bytes = request.ToByteArray();
        _ = await Assert.That(bytes[0]).IsEqualTo(byte.CreateChecked(0));
        var decoded = AppendReplicaEntriesRequest.Parser.ParseFrom(bytes);
        _ = await Assert.That(decoded.Entries[1].LogIndex).IsEqualTo(secondIndex);
        _ = await Assert.That(decoded.Entries[1].Term).IsEqualTo(secondTerm);
    }

    /// <summary>The last representable log index can be reconstructed.</summary>
    [Test]
    public async Task SequentialBatchCanEndAtMaximumIndex()
    {
        var request = new AppendReplicaEntriesRequest { PrevLogIndex = ulong.MaxValue - 1, Entries = [new(ulong.MaxValue, ulong.MaxValue, default)] };
        var bytes = request.ToByteArray();
        _ = await Assert.That(bytes[0]).IsEqualTo(byte.CreateChecked(2));
        _ = await Assert.That(AppendReplicaEntriesRequest.Parser.ParseFrom(bytes).Entries[0].LogIndex).IsEqualTo(ulong.MaxValue);
    }

    /// <summary>The term and commit position can be recovered from the envelope and previous index.</summary>
    [Test]
    public async Task ReconstructsSharedTermsAndCommitPosition()
    {
        var request = new AppendReplicaEntriesRequest
        {
            Header = new EnvelopeHeader { Term = 17 },
            PrevLogIndex = 128,
            PrevLogTerm = 17,
            LeaderCommitIndex = 128,
            Entries = [new(129, 17, new byte[] { 42 })],
        };
        byte[] expected = [31, 1, 0, 0, 0, 17, 0, 128, 1, 1, 1, 42];
        _ = await Assert.That(request.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        var parsed = AppendReplicaEntriesRequest.Parser.ParseFrom(expected);
        _ = await Assert.That((parsed.PrevLogTerm, parsed.LeaderCommitIndex, parsed.Entries[0].LogIndex, parsed.Entries[0].Term)).IsEqualTo((17UL, 128UL, 129UL, 17UL));

        request.Entries = [];
        byte[] heartbeat = [13, 1, 0, 0, 0, 17, 0, 128, 1, 0];
        _ = await Assert.That(request.ToByteArray().AsSpan().SequenceEqual(heartbeat)).IsTrue();
    }

    /// <summary>Empty sequential batches, overflowing indexes and truncated payloads are malformed.</summary>
    [Test]
    public void RejectsMalformedSequentialRange()
    {
        foreach (var bytes in new byte[][] { [4, 0, 0, 0], [16, 0, 0, 0], [17, 1, 0, 0, 0, 17, 0, 0, 0, 0, 0], [2, 0, 0, 0, 0], [2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 1, 0, 0, 1, 7, 0], [2, 0, 0, 0, 2, 7, 0] })
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes, static frame => _ = AppendReplicaEntriesRequest.Parser.ParseFrom(frame));
    }
}
