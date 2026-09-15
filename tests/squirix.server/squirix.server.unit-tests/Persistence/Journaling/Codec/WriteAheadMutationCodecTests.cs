using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Round-trip tests for journal frames carrying the write-ahead mutation operation id.</summary>
[Immutable]
public sealed class WriteAheadMutationCodecTests
{
    private const string OpId = "0123456789abcdef0123456789abcdef";

    /// <summary>Write-ahead started intent frames round-trip operation id and fingerprint.</summary>
    [Test]
    public async Task IdempotencyStartedRoundTrips()
    {
        var record = new JournalRecord
        {
            Sequence = 7,
            UnixMs = 2000,
            Operation = JournalOperationKind.IdempotencyStarted,
            Key = new CacheKey(string.Empty, string.Empty),
            IdempotencyOperationId = OpId,
            IdempotencyFingerprint = "try-add-entry-async|default|k|abc123",
        };

        var decoded = RoundTrip(record);

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.IdempotencyStarted);
        _ = await Assert.That(decoded.IdempotencyOperationId).IsEqualTo(OpId);
        _ = await Assert.That(decoded.IdempotencyFingerprint).IsEqualTo("try-add-entry-async|default|k|abc123");
    }

    /// <summary>A mutation prefix shorter than the length field is rejected.</summary>
    [Test]
    public void MutationOpIdLengthMissingThrows()
    {
        var body = FrameBody(6, [0x01]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>Oversized mutation operation ids are rejected during encode.</summary>
    [Test]
    public void MutationOpIdOversizedThrows()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1000,
            Operation = JournalOperationKind.Put,
            Key = new CacheKey("default", "k"),
            PutEntryBytes = new byte[] { 0x01 },
            MutationOperationId = new string('a', 70000),
        };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            record,
            static candidate =>
            {
                var prepared = BinaryJournalCodec.PrepareEncode(candidate);
                var body = new byte[prepared.BodyLength];
                _ = BinaryJournalCodec.Encode(candidate, body, in prepared);
            });
    }

    /// <summary>A truncated mutation operation id is rejected.</summary>
    [Test]
    public void MutationOpIdTruncatedThrows()
    {
        var body = FrameBody(6, [0x05, 0x00, 0x61, 0x62]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A mutation payload shorter than its operation-id prefix is rejected.</summary>
    [Test]
    public void MutationPayloadTruncatedThrows()
    {
        var body = FrameBody(7, [0x02, 0x00, 0xFF, 0xFF]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A put frame with malformed UTF-8 in the operation id is rejected even when lengths align.</summary>
    [Test]
    public void PutMalformedOpIdUtf8Throws()
    {
        var body = FrameBody(6, [0x02, 0x00, 0xFF, 0xFF, 0x01, 0x02, 0x03, 0x04]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A put payload shorter than its operation-id prefix is rejected.</summary>
    [Test]
    public void PutPayloadLengthMismatchThrows()
    {
        var body = FrameBody(6, [0x02, 0x00, 0xFF, 0xFF]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A put frame appended inside an idempotent RPC carries the mutation operation id.</summary>
    [Test]
    public async Task PutWithOperationIdRoundTrips()
    {
        var decoded = RoundTrip(CreateRecord(JournalOperationKind.Put, new byte[] { 0x01, 0x02, 0x03 }));

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(decoded.MutationOperationId).IsEqualTo(OpId);
        _ = await Assert.That(decoded.PutEntryBytes.Length).IsEqualTo(3);
    }

    /// <summary>Legacy mutation frames without an operation id still decode with a null operation id.</summary>
    [Test]
    public async Task PutWithoutOperationIdKeepsNullMutationId()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1000,
            Operation = JournalOperationKind.Put,
            Key = new CacheKey("default", "k"),
            PutEntryBytes = new byte[] { 0x01 },
        };

        var decoded = RoundTrip(record);

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(decoded.MutationOperationId).IsNull();
        _ = await Assert.That(decoded.PutEntryBytes.Length).IsEqualTo(1);
    }

    /// <summary>Remove-expiration frames carry the mutation operation id.</summary>
    [Test]
    public async Task RemoveExpirationOpIdRoundTrips()
    {
        var decoded = RoundTrip(CreateRecord(JournalOperationKind.RemoveExpiration));

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.RemoveExpiration);
        _ = await Assert.That(decoded.MutationOperationId).IsEqualTo(OpId);
    }

    /// <summary>Remove frames carry the mutation operation id.</summary>
    [Test]
    public async Task RemoveWithOperationIdRoundTrips()
    {
        var decoded = RoundTrip(CreateRecord(JournalOperationKind.Remove));

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.Remove);
        _ = await Assert.That(decoded.MutationOperationId).IsEqualTo(OpId);
    }

    /// <summary>A started frame with a truncated fingerprint is rejected.</summary>
    [Test]
    public void StartedFingerprintTruncatedThrows()
    {
        var body = FrameBody(10, [0x01, 0x00, 0x61, 0x05, 0x00, 0x62, 0x63]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A started frame with a truncated operation id is rejected.</summary>
    [Test]
    public void StartedOpIdTruncatedThrows()
    {
        var body = FrameBody(10, [0x03, 0x00, 0x61]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>Oversized started operation ids are rejected during encoding.</summary>
    [Test]
    public void StartedOversizedIdsThrow()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1000,
            Operation = JournalOperationKind.IdempotencyStarted,
            Key = new CacheKey(string.Empty, string.Empty),
            IdempotencyOperationId = new string('a', 70000),
            IdempotencyFingerprint = "fp",
        };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            record,
            static candidate =>
            {
                var prepared = BinaryJournalCodec.PrepareEncode(candidate);
                var body = new byte[prepared.BodyLength];
                _ = BinaryJournalCodec.Encode(candidate, body, in prepared);
            });
    }

    /// <summary>A started frame shorter than the operation-id length field is rejected.</summary>
    [Test]
    public void StartedPayloadMissingThrows()
    {
        var body = FrameBody(10, [0x01]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>Touch-expiration frames carry the mutation operation id and preserve the expiration.</summary>
    [Test]
    public async Task TouchExpirationWithOperationIdRoundTrips()
    {
        var expires = new DateTime(2026, 8, 1, 12, 0, 0, DateTimeKind.Utc);
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1000,
            Operation = JournalOperationKind.TouchExpiration,
            Key = new CacheKey("default", "k"),
            MutationOperationId = OpId,
            TouchExpirationUtc = expires,
        };

        var decoded = RoundTrip(record);

        _ = await Assert.That(decoded.Operation).IsEqualTo(JournalOperationKind.TouchExpiration);
        _ = await Assert.That(decoded.MutationOperationId).IsEqualTo(OpId);
        _ = await Assert.That(decoded.TouchExpirationUtc).IsEqualTo(expires);
    }

    /// <summary>A touch-expiration payload without the 8-byte expiration is rejected.</summary>
    [Test]
    public void TouchPayloadLengthMismatchThrows()
    {
        var body = FrameBody(9, [0x03, 0x00, 0x61, 0x62, 0x63]);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    /// <summary>A payload length overrunning the bounded frame body is rejected.</summary>
    [Test]
    public void TruncatedPayloadOverrunThrows()
    {
        var valid = FrameBody(7, [0x02, 0x00, 0x61, 0x62]);
        var padded = new byte[valid.Length + 16];
        valid.CopyTo(padded, 0);
        BinaryPrimitives.WriteInt32LittleEndian(padded.AsSpan(21), 100);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws((Buffer: padded, valid.Length), static state => _ = BinaryJournalCodec.Decode(state.Buffer, state.Length));
    }

    /// <summary>An unknown opcode wire value is rejected during decoding.</summary>
    [Test]
    public void UnknownOpcodeWireThrows()
    {
        var body = FrameBody(99, []);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = BinaryJournalCodec.Decode(buffer, buffer.Length));
    }

    private static JournalRecord CreateRecord(JournalOperationKind operation, ReadOnlyMemory<byte> putEntryBytes = default) => new()
    {
        Sequence = 1,
        UnixMs = 1000,
        Operation = operation,
        Key = new CacheKey("default", "k"),
        PutEntryBytes = putEntryBytes,
        MutationOperationId = OpId,
    };

    private static byte[] FrameBody(byte opcodeWire, byte[] payload, string ns = "default", string key = "k")
    {
        var nsBytes = Encoding.UTF8.GetBytes(ns);
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var body = new byte[25 + nsBytes.Length + keyBytes.Length + payload.Length];
        BinaryPrimitives.WriteUInt64LittleEndian(body, 1UL);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), 1000L);
        body[16] = opcodeWire;
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(17), ushort.CreateTruncating(nsBytes.Length));
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(19), ushort.CreateTruncating(keyBytes.Length));
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(21), payload.Length);
        nsBytes.CopyTo(body.AsSpan(25));
        keyBytes.CopyTo(body.AsSpan(25 + nsBytes.Length));
        payload.CopyTo(body.AsSpan(25 + nsBytes.Length + keyBytes.Length));
        return body;
    }

    private static JournalRecord RoundTrip(JournalRecord record)
    {
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var body = new byte[prepared.BodyLength];
        _ = BinaryJournalCodec.Encode(record, body, in prepared);
        return BinaryJournalCodec.Decode(body, body.Length);
    }
}
