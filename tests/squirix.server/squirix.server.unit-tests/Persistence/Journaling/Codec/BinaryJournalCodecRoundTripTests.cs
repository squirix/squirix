using System;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Codec;

/// <summary>Round-trip tests for <see cref="BinaryJournalCodec" /> encode paths.</summary>
public sealed class BinaryJournalCodecRoundTripTests
{
    /// <summary>Internal-only journal operations must not be prepared for on-disk encoding.</summary>
    [Fact]
    public void PrepareEncodeRejectsInternalOnlyOperations()
    {
        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.AwaitDurabilityCommit,
            Key = CacheKey.Default("k"),
        };

        var ex = Assert.Throws<NotSupportedException>(() => BinaryJournalCodec.PrepareEncode(record));
        Assert.Contains("AwaitDurabilityCommit", ex.Message, StringComparison.Ordinal);
    }

    /// <summary>Idempotency outcome journal records round-trip through PrepareEncode, Encode, and Decode.</summary>
    [Fact]
    public void PrepareEncodeRoundTripsIdempotencyOutcome() => PrepareEncodeRoundTripsDecodeCore(JournalOperationKind.IdempotencyOutcome);

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

    private static JournalRecord CreateRecord(JournalOperationKind operation)
    {
        var key = new CacheKey("ns", "codec-key");
        switch (operation)
        {
            case JournalOperationKind.Put:
                return new JournalRecord
                {
                    Sequence = 1,
                    UnixMs = 123,
                    Operation = JournalOperationKind.Put,
                    Key = key,
                    PutEntryBytes = JournalEntryPayloadKit.EncodePut("value"),
                };
            case JournalOperationKind.Remove:
                return new JournalRecord
                {
                    Sequence = 2,
                    UnixMs = 123,
                    Operation = JournalOperationKind.Remove,
                    Key = key,
                };
            case JournalOperationKind.RemoveExpiration:
                return new JournalRecord
                {
                    Sequence = 3,
                    UnixMs = 123,
                    Operation = JournalOperationKind.RemoveExpiration,
                    Key = key,
                };
            case JournalOperationKind.TouchExpiration:
                return new JournalRecord
                {
                    Sequence = 4,
                    UnixMs = 123,
                    Operation = JournalOperationKind.TouchExpiration,
                    Key = key,
                    TouchExpirationUtc = new DateTime(2026, 6, 30, 12, 0, 0, DateTimeKind.Utc),
                };
            case JournalOperationKind.IdempotencyOutcome:
                return new JournalRecord
                {
                    Sequence = 5,
                    UnixMs = 123,
                    Operation = JournalOperationKind.IdempotencyOutcome,
                    Key = new CacheKey(string.Empty, string.Empty),
                    IdempotencyOperationId = "0123456789abcdef0123456789abcdef",
                    IdempotencyFingerprint = "try-add-entry-async|default|k|abc123",
                    IdempotencyResponseBytes = new byte[] { 0x08, 0x01 },
                };
            case JournalOperationKind.AwaitDurabilityCommit:
            case JournalOperationKind.WaitForStartup:
            case JournalOperationKind.MaintenanceExclusive:
            case JournalOperationKind.SnapshotCut:
            case JournalOperationKind.UnderSnapshotBarrier:
            default:
                throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported encodable operation.");
        }
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

        if (operation is not JournalOperationKind.IdempotencyOutcome)
            return;
        Assert.Equal(record.IdempotencyOperationId, decoded.IdempotencyOperationId);
        Assert.Equal(record.IdempotencyFingerprint, decoded.IdempotencyFingerprint);
        Assert.Equal(record.IdempotencyResponseBytes.Length, decoded.IdempotencyResponseBytes.Length);
    }
}
