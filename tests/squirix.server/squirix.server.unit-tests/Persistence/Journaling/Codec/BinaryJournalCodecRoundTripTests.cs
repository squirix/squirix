using System;
using System.Buffers.Binary;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Round-trip tests for <see cref="BinaryJournalCodec" /> encode paths.</summary>
[Immutable]
public sealed class BinaryJournalCodecRoundTripTests
{
    private static readonly byte[] IdempotencyResponseFixture = [0x08, 0x01];

    private static readonly byte[] TruncatedFrameBody = [0x01, 0x02, 0x03, 0x04];

    /// <summary>Decode rejects truncated frame bodies.</summary>
    [Test]
    public async Task DecodeRejectsTruncatedFrameBody()
    {
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(TruncatedFrameBody, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        _ = await Assert.That(ex.Message).Contains("truncated", StringComparison.Ordinal);
    }

    /// <summary>Decode rejects put frames whose payload length exceeds the buffer.</summary>
    [Test]
    public async Task DecodeRejectsTruncatedPutPayload()
    {
        var record = CreateRecord(JournalOperationKind.Put);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
        BinaryPrimitives.WriteInt32LittleEndian(bodyBytes.AsSpan(21), bodyBytes.Length);
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(bodyBytes, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        _ = await Assert.That(ex.Message).Contains("truncated", StringComparison.Ordinal);
    }

    /// <summary>Decode rejects unknown opcodes.</summary>
    [Test]
    public async Task DecodeRejectsUnknownOpcode()
    {
        var record = CreateRecord(JournalOperationKind.Remove);
        var prepared = BinaryJournalCodec.PrepareEncode(record);
        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));
        bodyBytes[16] = 0xFF;
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(bodyBytes, static value => _ = BinaryJournalCodec.Decode(value, value.Length));
        _ = await Assert.That(ex.Message).Contains("Unknown journal opcode", StringComparison.Ordinal);
    }

    /// <summary>Encode rejects an idempotency fingerprint that cannot fit the on-disk length prefix rather than silently truncating it.</summary>
    [Test]
    public async Task EncodeRejectsOversizedFingerprint()
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
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            (record, prepared),
            static ctx =>
            {
                var body = new byte[ctx.prepared.BodyLength];
                _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared);
            });
        _ = await Assert.That(ex.Message).Contains("maximum encoded length", StringComparison.Ordinal);
    }

    /// <summary>Internal-only journal operations must not be prepared for on-disk encoding.</summary>
    [Test]
    public async Task PrepareEncodeRejectsInternalOnlyOps()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.AwaitDurabilityCommit,
            Key = CacheKey.Default("k"),
        };

        var ex = NodeExceptionAssert.For<NotSupportedException>().Throws(record, static value => BinaryJournalCodec.PrepareEncode(value));
        _ = await Assert.That(ex.Message).Contains("cannot be determined", StringComparison.Ordinal);
    }

    /// <summary>Idempotency outcome journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Test]
    public Task PrepareEncodeRoundTripsIdempotency() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.IdempotencyOutcome);

    /// <summary>Put journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Test]
    public Task PrepareEncodeRoundTripsPut() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.Put);

    /// <summary>Remove journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Test]
    public Task PrepareEncodeRoundTripsRemove() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.Remove);

    /// <summary>Remove-expiration journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Test]
    public Task PrepareEncodeRoundTripsRemoveExpiration() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.RemoveExpiration);

    /// <summary>Touch-expiration journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Test]
    public Task PrepareEncodeRoundTripsTouchExpiration() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.TouchExpiration);

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
            JournalOperationKind.IdempotencyStarted => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported encodable operation."),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported encodable operation."),
        };
    }

    private static async Task PrepareEncodeRoundTripsDecodeCore(JournalOperationKind operation)
    {
        var record = CreateRecord(operation);
        var prepared = BinaryJournalCodec.PrepareEncode(record);

        // Golden frame-body lengths from the documented wire layout
        // (FixedPrefixSize 25 + UTF-8 key bytes + operation payload), not from
        // BinaryJournalCodec.ComputeFrameBodyLength: both helpers share the same
        // EncodeContext.From path, so comparing them could never fail.
        var expectedBodyLength = operation switch
        {
            // 25 prefix + 11 key ("ns" + "codec-key") + 22 payload:
            // flags (2) + version (8) + empty tags (2) + string "value" (1 + 4 + 5).
            JournalOperationKind.Put => 58,
            JournalOperationKind.Remove => 36,
            JournalOperationKind.RemoveExpiration => 36,
            JournalOperationKind.TouchExpiration => 44,
            JournalOperationKind.IdempotencyOutcome => 103,
            JournalOperationKind.AwaitDurabilityCommit or JournalOperationKind.WaitForStartup or JournalOperationKind.MaintenanceExclusive or JournalOperationKind.SnapshotCut
                or JournalOperationKind.UnderSnapshotBarrier => throw new ArgumentOutOfRangeException(
                    nameof(operation),
                    operation,
                    "No golden body length for internal-only operation."),
            JournalOperationKind.IdempotencyStarted => throw new ArgumentOutOfRangeException(nameof(operation), operation, "No golden body length for operation."),
            _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "No golden body length for operation."),
        };
        _ = await Assert.That(prepared.BodyLength).IsEqualTo(expectedBodyLength);

        var bodyBytes = BufferKit.ToOwnedBytes(prepared.BodyLength, (record, prepared), static (ctx, body) => _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.prepared));

        var decoded = BinaryJournalCodec.Decode(bodyBytes, bodyBytes.Length);
        _ = await Assert.That(decoded.Operation).IsEqualTo(operation);
        _ = await Assert.That(decoded.Key.Namespace).IsEqualTo(record.Key.Namespace);
        _ = await Assert.That(decoded.Key.Key).IsEqualTo(record.Key.Key);

        if (operation is JournalOperationKind.Put)
            _ = await Assert.That(decoded.PutEntryBytes.Length).IsEqualTo(record.PutEntryBytes.Length);

        if (operation is JournalOperationKind.TouchExpiration)
            _ = await Assert.That(decoded.TouchExpirationUtc).IsEqualTo(record.TouchExpirationUtc);

        if (operation != JournalOperationKind.IdempotencyOutcome)
            return;
        _ = await Assert.That(decoded.IdempotencyOperationId).IsEqualTo(record.IdempotencyOperationId);
        _ = await Assert.That(decoded.IdempotencyFingerprint).IsEqualTo(record.IdempotencyFingerprint);
        _ = await Assert.That(decoded.IdempotencyResponseBytes.Length).IsEqualTo(record.IdempotencyResponseBytes.Length);
    }
}
