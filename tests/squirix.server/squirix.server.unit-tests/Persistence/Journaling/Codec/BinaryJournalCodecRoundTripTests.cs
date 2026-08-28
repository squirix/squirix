using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Round-trip tests for <see cref="BinaryJournalCodec" /> encode paths.</summary>
[Immutable]
public sealed class BinaryJournalCodecRoundTripTests
{
    private static readonly byte[] IdempotencyResponseFixture = [0x08, 0x01];

    private static readonly byte[] TruncatedFrameBody = [0x01, 0x02, 0x03, 0x04];

    /// <summary>Decode rejects truncated frame bodies.</summary>
    [Fact]
    public void DecodeRejectsTruncatedFrameBody()
    {
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(TruncatedFrameBody, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        Assert.Contains("truncated", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Decode rejects put frames whose payload length exceeds the buffer.</summary>
    [Fact]
    public void DecodeRejectsTruncatedPutPayload()
    {
        var record = CreateRecord(JournalOperationKind.Put);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
        BinaryPrimitives.WriteInt32LittleEndian(bodyBytes.AsSpan(21), bodyBytes.Length);
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(bodyBytes, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        Assert.Contains("truncated", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Decode rejects unknown opcodes.</summary>
    [Fact]
    public void DecodeRejectsUnknownOpcode()
    {
        var record = CreateRecord(JournalOperationKind.Remove);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
        bodyBytes[16] = 0xFF;
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(bodyBytes, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        Assert.Contains("Unknown journal opcode", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Internal-only journal operations must not be prepared for on-disk encoding.</summary>
    [Fact]
    public void PrepareEncodeRejectsInternalOnlyOps()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.AwaitDurabilityCommit,
            Key = CacheKey.Default("k"),
        };

        var ex = NodeExceptionAssert.For<NotSupportedException>().Throws(record, static value => BinaryJournalCodec.PrepareEncode(value));
        Assert.Contains("cannot be determined", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Idempotency outcome journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsIdempotency() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.IdempotencyOutcome);

    /// <summary>Put journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsPut() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.Put);

    /// <summary>Remove journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsRemove() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.Remove);

    /// <summary>Remove-expiration journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsRemoveExpiration() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.RemoveExpiration);

    /// <summary>Touch-expiration journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsTouchExpiration() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.TouchExpiration);

    /// <summary>Encode rejects an idempotency fingerprint that cannot fit the on-disk length prefix rather than silently truncating it.</summary>
    [Fact]
    public void EncodeRejectsOversizedFingerprint()
    {
        var record = new JournalRecord
        {
            Sequence = 6,
            UnixMs = 123,
            Operation = JournalOperationKind.IdempotencyOutcome,
            Key = new CacheKey(string.Empty, string.Empty),
            IdempotencyOperationId = "0123456789abcdef0123456789abcdef",
            IdempotencyFingerprint = new string('x', ushort.MaxValue + 1),
            IdempotencyResponseBytes = IdempotencyResponseFixture,
        };

        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws((record, prepared), static ctx =>
        {
            var body = new byte[ctx.prepared.BodyLength];
            _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared);
        });
        Assert.Contains("maximum encoded length", ex.Message, StringComparison.Ordinal);
    }

    private static JournalRecord CreateRecord(JournalOperationKind operation)
    {
        var key = new CacheKey("ns", "codec-key");
        return operation switch
        {
            JournalOperationKind.Put => new JournalRecord
            {
                Sequence = 1,
                UnixMs = 123,
                Operation = JournalOperationKind.Put,
                Key = key,
                PutEntryBytes = JournalEntryPayloadKit.EncodePut("value"),
            },
            JournalOperationKind.Remove => new JournalRecord
            {
                Sequence = 2,
                UnixMs = 123,
                Operation = JournalOperationKind.Remove,
                Key = key,
            },
            JournalOperationKind.RemoveExpiration => new JournalRecord
            {
                Sequence = 3,
                UnixMs = 123,
                Operation = JournalOperationKind.RemoveExpiration,
                Key = key,
            },
            JournalOperationKind.TouchExpiration => new JournalRecord
            {
                Sequence = 4,
                UnixMs = 123,
                Operation = JournalOperationKind.TouchExpiration,
                Key = key,
                TouchExpirationUtc = new DateTime(2026, 6, 30, 12, 0, 0, DateTimeKind.Utc),
            },
            JournalOperationKind.IdempotencyOutcome => new JournalRecord
            {
                Sequence = 5,
                UnixMs = 123,
                Operation = JournalOperationKind.IdempotencyOutcome,
                Key = new CacheKey(string.Empty, string.Empty),
                IdempotencyOperationId = "0123456789abcdef0123456789abcdef",
                IdempotencyFingerprint = "try-add-entry-async|default|k|abc123",
                IdempotencyResponseBytes = IdempotencyResponseFixture,
            },
            JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.MaintenanceExclusive or JournalOperationKind.SnapshotCut
                or JournalOperationKind.UnderSnapshotBarrier => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported encodable operation."),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported encodable operation."),
        };
    }

    private static void PrepareEncodeRoundTripsDecodeCore(JournalOperationKind operation)
    {
        var record = CreateRecord(operation);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        Assert.Equal(BinaryJournalCodec.ComputeFrameBodyLength(record), prepared.BodyLength);

        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));

        var decoded = BinaryJournalCodec.Decode(bodyBytes, bodyBytes.Length);
        Assert.Equal(operation, decoded.Operation);
        Assert.Equal(record.Key.Namespace, decoded.Key.Namespace);
        Assert.Equal(record.Key.Key, decoded.Key.Key);

        if (operation is JournalOperationKind.Put)
            Assert.Equal(record.PutEntryBytes.Length, decoded.PutEntryBytes.Length);

        if (operation is JournalOperationKind.TouchExpiration)
            Assert.Equal(record.TouchExpirationUtc, decoded.TouchExpirationUtc);

        if (operation != JournalOperationKind.IdempotencyOutcome)
            return;
        Assert.Equal(record.IdempotencyOperationId, decoded.IdempotencyOperationId);
        Assert.Equal(record.IdempotencyFingerprint, decoded.IdempotencyFingerprint);
        Assert.Equal(record.IdempotencyResponseBytes.Length, decoded.IdempotencyResponseBytes.Length);
    }
}
