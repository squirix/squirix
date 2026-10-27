using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Round-trip tests for the idempotency snapshot codec (completed and write-ahead started records).</summary>
[Immutable]
public sealed class IdempotencyCodecTests : ServerUnitTestBase
{
    private static readonly byte[] OneByteResponse = [1];

    private static readonly byte[] SampleResponse = [0x08, 0x01];

    /// <summary>Length computation matches the documented golden size for a fixed record.</summary>
    [Test]
    public async Task ComputeEncodedLengthMatchesGolden()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            SampleResponse,
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        // 1 (state) + 2 + 32 + 2 + 36 + 8 + 4 + 2.
        _ = await Assert.That(IdempotencyCodec.ComputeEncodedLength(record)).IsEqualTo(87);
    }

    /// <summary>Length computation rejects oversized UTF-8 fields.</summary>
    [Test]
    public async Task EncodedLengthRejectsOversizedOpId()
    {
        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            ushort.MaxValue + 1,
            static length =>
            {
                var record = new PersistedIdempotencyRecord(new string('a', length), "fp", OneByteResponse, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
                _ = IdempotencyCodec.ComputeEncodedLength(record);
            });
        _ = await Assert.That(ex.Message).Contains("maximum encoded length", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>A state byte outside the versioned layout is rejected.</summary>
    [Test]
    public void InvalidStateByteThrows()
    {
        var body = new byte[] { 0x05, 0x00 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>Pre-upgrade snapshots without the leading state byte still decode.</summary>
    [Test]
    public async Task LegacyCompletedRecordReads()
    {
        var createdUtc = new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc);
        var body = LegacyWireBytes("0123456789abcdef0123456789abcdef", "try-add-entry-async|default|k|abc123", createdUtc, SampleResponse);

        var decoded = IdempotencyCodec.Read(body);

        _ = await Assert.That(decoded.State).IsEqualTo(IdempotencyRecordState.Completed);
        _ = await Assert.That(decoded.OperationId).IsEqualTo("0123456789abcdef0123456789abcdef");
        _ = await Assert.That(decoded.Fingerprint).IsEqualTo("try-add-entry-async|default|k|abc123");
        _ = await Assert.That(decoded.CreatedUtc).IsEqualTo(createdUtc);
        await SequenceAssert.EqualAsync<byte>([0x08, 0x01], decoded.ResponseBytes);
    }

    /// <summary>A legacy record without fingerprint bytes is rejected.</summary>
    [Test]
    public void LegacyMissingFingerprintThrows()
    {
        var body = new byte[] { 0x02, 0x00, 0x6F, 0x70 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>Read decodes the golden wire bytes into the fixed record fields.</summary>
    [Test]
    public async Task ReadReadsGoldenWireBytes()
    {
        var decoded = IdempotencyCodec.Read(GoldenWireBytes());

        _ = await Assert.That(decoded.OperationId).IsEqualTo("0123456789abcdef0123456789abcdef");
        _ = await Assert.That(decoded.Fingerprint).IsEqualTo("try-add-entry-async|default|k|abc123");
        _ = await Assert.That(decoded.CreatedUtc).IsEqualTo(new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        await SequenceAssert.EqualAsync<byte>([0x08, 0x01], decoded.ResponseBytes);
        _ = await Assert.That(decoded.State).IsEqualTo(IdempotencyRecordState.Completed);
    }

    /// <summary>Truncated buffer must throw InvalidDataException.</summary>
    [Test]
    public void ReadThrowsOnTruncatedBuffer() => _ = NodeExceptionAssert.For<InvalidDataException>().Throws(0, static _ => IdempotencyCodec.Read([]));

    /// <summary>Empty response bytes are rejected during validation on read.</summary>
    [Test]
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

    /// <summary>A started record with an invalid fingerprint presence flag is rejected.</summary>
    [Test]
    public void StartedBadFlagThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70, 0x02 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>A started record declaring a fingerprint without its bytes is rejected.</summary>
    [Test]
    public void StartedMissingFingerprintThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70, 0x01 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>A started record without the fingerprint presence flag is rejected.</summary>
    [Test]
    public void StartedMissingFlagThrows()
    {
        var body = new byte[] { 0x01, 0x02, 0x00, 0x6F, 0x70 };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(body, static buffer => _ = IdempotencyCodec.Read(buffer));
    }

    /// <summary>Length computation rejects oversized started fingerprints.</summary>
    [Test]
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

    /// <summary>Length computation rejects oversized started operation ids.</summary>
    [Test]
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

    /// <summary>Started records round-trip including the fingerprint when present.</summary>
    [Test]
    public async Task StartedRecordRoundTripWithFp()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var bytes = BufferKit.ToOwnedBytes(IdempotencyCodec.ComputeEncodedLength(record), record, static (state, buffer) => IdempotencyCodec.Write(state, buffer));
        var decoded = IdempotencyCodec.Read(bytes);

        _ = await Assert.That(decoded.State).IsEqualTo(IdempotencyRecordState.Started);
        _ = await Assert.That(decoded.OperationId).IsEqualTo(record.OperationId);
        _ = await Assert.That(decoded.Fingerprint).IsEqualTo(record.Fingerprint);
        _ = await Assert.That(decoded.CreatedUtc).IsEqualTo(record.CreatedUtc);
        _ = await Assert.That(decoded.ResponseBytes).IsEmpty();
    }

    /// <summary>Started records reconstructed from journal mutation frames have no fingerprint and round-trip.</summary>
    [Test]
    public async Task StartedRecordRoundTripWithoutFp()
    {
        var record = new PersistedIdempotencyRecord("0123456789abcdef0123456789abcdef", null, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var bytes = BufferKit.ToOwnedBytes(IdempotencyCodec.ComputeEncodedLength(record), record, static (state, buffer) => IdempotencyCodec.Write(state, buffer));
        var decoded = IdempotencyCodec.Read(bytes);

        _ = await Assert.That(decoded.State).IsEqualTo(IdempotencyRecordState.Started);
        _ = await Assert.That(decoded.OperationId).IsEqualTo(record.OperationId);
        _ = await Assert.That(decoded.Fingerprint).IsNull();
        _ = await Assert.That(decoded.CreatedUtc).IsEqualTo(record.CreatedUtc);
        _ = await Assert.That(decoded.ResponseBytes).IsEmpty();
    }

    /// <summary>Trailing bytes after a valid record are rejected.</summary>
    [Test]
    public void TrailingByteThrows()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            SampleResponse,
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var buffer = new byte[IdempotencyCodec.ComputeEncodedLength(record) + 1];
        IdempotencyCodec.Write(record, buffer);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(buffer, static candidate => _ = IdempotencyCodec.Read(candidate));
    }

    /// <summary>Encodes and decodes an idempotency record with response bytes.</summary>
    [Test]
    public async Task WriteAndReadRoundTripsResponseBytes()
    {
        var response = new TryAddAsyncResponse { Added = true };
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            response.ToByteArray(),
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));

        var bytes = BufferKit.ToOwnedBytes(IdempotencyCodec.ComputeEncodedLength(record), record, static (state, buffer) => IdempotencyCodec.Write(state, buffer));
        var decoded = IdempotencyCodec.Read(bytes);

        _ = await Assert.That(decoded.OperationId).IsEqualTo(record.OperationId);
        _ = await Assert.That(decoded.Fingerprint).IsEqualTo(record.Fingerprint);
        _ = await Assert.That(decoded.CreatedUtc).IsEqualTo(record.CreatedUtc);
        _ = await Assert.That(decoded.ResponseBytes.Length).IsEqualTo(record.ResponseBytes.Length);

        var replayed = TryAddAsyncResponse.Parser.ParseFrom(decoded.ResponseBytes);
        _ = await Assert.That(replayed.Added).IsTrue();
    }

    /// <summary>Write emits the documented golden wire bytes for a fixed record.</summary>
    [Test]
    public Task WriteMatchesGoldenWireBytes()
    {
        var record = new PersistedIdempotencyRecord(
            "0123456789abcdef0123456789abcdef",
            "try-add-entry-async|default|k|abc123",
            SampleResponse,
            new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var buffer = new byte[IdempotencyCodec.ComputeEncodedLength(record)];

        IdempotencyCodec.Write(record, buffer);

        return SequenceAssert.EqualAsync(GoldenWireBytes(), buffer);
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
}
