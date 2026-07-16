using System;
using System.IO;
using Google.Protobuf;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Snapshot.Binary;

/// <summary>Round-trip tests for the v2 idempotency snapshot codec.</summary>
public sealed class IdempotencyCodecTests : UnitTestBase
{
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

    /// <summary>Truncated buffer must throw InvalidDataException.</summary>
    [Fact]
    public void ReadThrowsOnTruncatedBuffer() => _ = Assert.Throws<InvalidDataException>(static () => IdempotencyCodec.Read([]));
}
