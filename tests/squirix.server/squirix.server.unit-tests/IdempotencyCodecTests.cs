using System;
using System.IO;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests;

/// <summary>Round-trip tests for the v2 idempotency snapshot codec.</summary>
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

        // 2 + 32 + 2 + 36 + 8 + 4 + 2.
        Assert.Equal(86, IdempotencyCodec.ComputeEncodedLength(record));
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

    /// <summary>
    /// Hand-built golden encoding of the fixed record above: u16-prefixed operation id
    /// (32 ASCII bytes), u16-prefixed fingerprint (36 ASCII bytes), i64 little-endian
    /// unix milliseconds for 2026-07-01T12:00:00Z, i32 little-endian response length (2),
    /// then the 2 response bytes. A symmetric write/read bug that a pure round-trip
    /// cannot see fails against these bytes.
    /// </summary>
    private static byte[] GoldenWireBytes() =>
    [
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
