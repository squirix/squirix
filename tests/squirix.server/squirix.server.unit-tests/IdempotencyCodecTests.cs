using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Round-trip tests for the idempotency snapshot codec (completed and write-ahead started records).</summary>
[Immutable]
public sealed class IdempotencyCodecTests : ServerUnitTestBase
{
    private static readonly byte[] OneByteResponse = [1];

    /// <summary>Length computation rejects oversized UTF-8 fields.</summary>
    [Fact]
    public void EncodedLengthRejectsOversizedOpId()
    {
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            ushort.MaxValue + 1,
            static length =>
            {
                var record = new PersistedIdempotencyRecord(new string('a', length), "fp", OneByteResponse, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
                _ = IdempotencyCodec.ComputeEncodedLength(record);
            });
        Assert.Contains("maximum encoded length", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Truncated buffer must throw InvalidDataException.</summary>
    [Fact]
    public void ReadThrowsOnTruncatedBuffer() => _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => IdempotencyCodec.Read([]));

    /// <summary>Empty response bytes are rejected during validation on read.</summary>
    [Fact]
    public void ReadThrowsWhenResponseBytesAreEmpty()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            [],
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var length = IdempotencyCodec.ComputeEncodedLength(record);
        Span<byte> buffer = stackalloc byte[length];
        IdempotencyCodec.Write(record, buffer);

        try
        {
            _ = IdempotencyCodec.Read(buffer);
            Assert.Fail("Expected InvalidDataException for empty response bytes.");
        }
        catch (InvalidDataException)
        {
            // expected
        }
    }

    /// <summary>Length computation matches the documented golden size for a fixed record.</summary>
    [Fact]
    public void ComputeEncodedLengthMatchesGolden()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            [0x08, 0x01],
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        // 1 (state) + 2 + 32 + 2 + 36 + 8 + 4 + 2.
        Assert.Equal(87, IdempotencyCodec.ComputeEncodedLength(record));
    }

    /// <summary>Write emits the documented golden wire bytes for a fixed record.</summary>
    [Fact]
    public void WriteMatchesGoldenWireBytes()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            [0x08, 0x01],
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var buffer = new byte[IdempotencyCodec.ComputeEncodedLength(record)];

        IdempotencyCodec.Write(record, buffer);

        Assert.Equal(GoldenWireBytes(), buffer);
    }

    /// <summary>Read decodes the golden wire bytes into the fixed record fields.</summary>
    [Fact]
    public void ReadReadsGoldenWireBytes()
    {
        var decoded = IdempotencyCodec.Read(GoldenWireBytes());

        Assert.Equal("0123456789abcdef0123456789abcdef", decoded.OperationId);
        Assert.Equal("try-add-entry-async|default|k|abc123", decoded.Fingerprint);
        Assert.Equal(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc), decoded.CreatedUtc);
        Assert.Equal(new byte[] { 0x08, 0x01 }, decoded.ResponseBytes);
        Assert.Equal(IdempotencyRecordState.Completed, decoded.State);
    }

    /// <summary>Encodes and decodes an idempotency record with response bytes.</summary>
    [Fact]
    public void WriteAndReadRoundTripsResponseBytes()
    {
        var response = new TryAddAsyncResponse { Added = true };
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            response.ToByteArray(),
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var length = IdempotencyCodec.ComputeEncodedLength(record);
        Span<byte> buffer = stackalloc byte[length];
        IdempotencyCodec.Write(record, buffer);

        var decoded = IdempotencyCodec.Read(buffer);

        Assert.Equal(record.OperationId, decoded.OperationId);
        Assert.Equal(record.Fingerprint, decoded.Fingerprint);
        Assert.Equal(record.CreatedUtc, decoded.CreatedUtc);
        Assert.Equal(record.ResponseBytes.Length, decoded.ResponseBytes.Length);

        var replayed = TryAddAsyncResponse.Parser.ParseFrom(decoded.ResponseBytes);
        Assert.True(replayed.Added);
    }

    /// <summary>Started records round-trip including the fingerprint when present.</summary>
    [Fact]
    public void StartedRecordRoundTripWithFp()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var length = IdempotencyCodec.ComputeEncodedLength(record);
        Span<byte> buffer = stackalloc byte[length];
        IdempotencyCodec.Write(record, buffer);

        var decoded = IdempotencyCodec.Read(buffer);

        Assert.Equal(IdempotencyRecordState.Started, decoded.State);
        Assert.Equal(record.OperationId, decoded.OperationId);
        Assert.Equal(record.Fingerprint, decoded.Fingerprint);
        Assert.Equal(record.CreatedUtc, decoded.CreatedUtc);
        Assert.Empty(decoded.ResponseBytes);
    }

    /// <summary>Started records reconstructed from journal mutation frames have no fingerprint and round-trip.</summary>
    [Fact]
    public void StartedRecordRoundTripWithoutFp()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            null,
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var length = IdempotencyCodec.ComputeEncodedLength(record);
        Span<byte> buffer = stackalloc byte[length];
        IdempotencyCodec.Write(record, buffer);

        var decoded = IdempotencyCodec.Read(buffer);

        Assert.Equal(IdempotencyRecordState.Started, decoded.State);
        Assert.Equal(record.OperationId, decoded.OperationId);
        Assert.Null(decoded.Fingerprint);
        Assert.Equal(record.CreatedUtc, decoded.CreatedUtc);
        Assert.Empty(decoded.ResponseBytes);
    }

    /// <summary>Pre-upgrade snapshots without the leading state byte still decode.</summary>
    [Fact]
    public void LegacyCompletedRecordReads()
    {
        var createdUtc = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var body = LegacyWireBytes(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            createdUtc,
            [0x08, 0x01]);

        var decoded = IdempotencyCodec.Read(body);

        Assert.Equal(IdempotencyRecordState.Completed, decoded.State);
        Assert.Equal("0123456789abcdef0123456789abcdef", decoded.OperationId);
        Assert.Equal("try-add-entry-async|default|k|abc123", decoded.Fingerprint);
        Assert.Equal(createdUtc, decoded.CreatedUtc);
        Assert.Equal(new byte[] { 0x08, 0x01 }, decoded.ResponseBytes);
    }

    /// <summary>A state byte outside the versioned layout is rejected.</summary>
    [Fact]
    public void InvalidStateByteThrows()
    {
        var body = new byte[] { 0x05, 0x00 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            body,
            static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>A started record without the fingerprint presence flag is rejected.</summary>
    [Fact]
    public void StartedMissingFlagThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            body,
            static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>A started record with an invalid fingerprint presence flag is rejected.</summary>
    [Fact]
    public void StartedBadFlagThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70, 0x02 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            body,
            static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>A started record declaring a fingerprint without its bytes is rejected.</summary>
    [Fact]
    public void StartedMissingFingerprintThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70, 0x01 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            body,
            static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>Trailing bytes after a valid record are rejected.</summary>
    [Fact]
    public void TrailingByteThrows()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            [0x08, 0x01],
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var buffer = new byte[IdempotencyCodec.ComputeEncodedLength(record) + 1];
        IdempotencyCodec.Write(record, buffer);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            buffer,
            static candidate => _ = IdempotencyCodec.Read(candidate));
    }

    /// <summary>Length computation rejects oversized started operation ids.</summary>
    [Fact]
    public void StartedOversizedOpIdThrows()
    {
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            ushort.MaxValue + 1,
            static length =>
            {
                var record = new PersistedIdempotencyRecord(new string('a', length), "fp", new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
                _ = IdempotencyCodec.ComputeEncodedLength(record);
            });
    }

    /// <summary>Length computation rejects oversized started fingerprints.</summary>
    [Fact]
    public void StartedOversizedFingerprintThrows()
    {
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            ushort.MaxValue + 1,
            static length =>
            {
                var record = new PersistedIdempotencyRecord("op", new string('f', length), new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
                _ = IdempotencyCodec.ComputeEncodedLength(record);
            });
    }

    /// <summary>A legacy record without fingerprint bytes is rejected.</summary>
    [Fact]
    public void LegacyMissingFingerprintThrows()
    {
        var body = new byte[] { 0x02, 0x00, 0x6F, 0x70 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            body,
            static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    private static byte[] LegacyWireBytes(string operationId, string fingerprint, DateTime createdUtc, byte[] response)
    {
        var operationIdBytes = Encoding.UTF8.GetBytes(operationId);
        var fingerprintBytes = Encoding.UTF8.GetBytes(fingerprint);
        var body = new byte[2 + operationIdBytes.Length + 2 + fingerprintBytes.Length + 8 + 4 + response.Length];
        var offset = 0;
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(offset), ushort.CreateTruncating(operationIdBytes.Length));
        offset += 2;
        operationIdBytes.CopyTo(body.AsSpan(offset));
        offset += operationIdBytes.Length;
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(offset), ushort.CreateTruncating(fingerprintBytes.Length));
        offset += 2;
        fingerprintBytes.CopyTo(body.AsSpan(offset));
        offset += fingerprintBytes.Length;
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(offset), new DateTimeOffset(createdUtc.ToUniversalTime()).ToUnixTimeMilliseconds());
        offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(body.AsSpan(offset), response.Length);
        offset += 4;
        response.CopyTo(body.AsSpan(offset));
        return body;
    }

    /// <summary>
    /// Hand-built golden encoding of the fixed record above: state byte (0 = completed), u16-prefixed
    /// operation id (32 ASCII bytes), u16-prefixed fingerprint (36 ASCII bytes), i64 little-endian
    /// unix milliseconds for 2026-07-01T12:00:00Z, i32 little-endian response length (2), then the
    /// 2 response bytes. A symmetric write/read bug that a pure round-trip cannot see fails against
    /// these bytes.
    /// </summary>
    private static byte[] GoldenWireBytes() =>
    [
        0x00,
        0x20, 0x00,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
        0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x61, 0x62, 0x63, 0x64, 0x65, 0x66,
        0x24, 0x00,
        0x74, 0x72, 0x79, 0x2D, 0x61, 0x64, 0x64, 0x2D, 0x65, 0x6E, 0x74, 0x72, 0x79, 0x2D, 0x61, 0x73, 0x79, 0x6E, 0x63,
        0x7C, 0x64, 0x65, 0x66, 0x61, 0x75, 0x6C, 0x74, 0x7C, 0x6B, 0x7C, 0x61, 0x62, 0x63, 0x31, 0x32, 0x33,
        0x00, 0xE2, 0x8C, 0x1D, 0x9F, 0x01, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00, 0x08, 0x01,
    ];
}
