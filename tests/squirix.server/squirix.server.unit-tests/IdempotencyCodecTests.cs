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
}
