using System;
using System.Buffers.Binary;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Out-of-range field values in a snapshot idempotency record are reported as damage, not as framework range exceptions.</summary>
[Immutable]
public sealed class IdempotencyCodecRangeTests : ServerUnitTestBase
{
    private static readonly byte[] SampleResponse = [0x08, 0x01];

    /// <summary>A created timestamp no DateTimeOffset holds is rejected, on the current and the legacy read path alike.</summary>
    [Test]
    public void ReadRejectsCreatedOutOfRange()
    {
        var bytes = EncodeCompleted();
        BinaryPrimitives.WriteInt64LittleEndian(bytes.AsSpan(bytes.Length - SampleResponse.Length - sizeof(int) - sizeof(long)), long.MaxValue);
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes, static value => IdempotencyCodec.Read(value));
    }

    /// <summary>A response length whose end overflows the bounds check is rejected.</summary>
    [Test]
    public void ReadRejectsResponseOverflow()
    {
        var bytes = EncodeCompleted();
        BinaryPrimitives.WriteInt32LittleEndian(bytes.AsSpan(bytes.Length - SampleResponse.Length - sizeof(int)), int.MaxValue);
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes, static value => IdempotencyCodec.Read(value));
    }

    /// <summary>Encodes a completed record whose layout ends with the created timestamp, the response length and the response bytes.</summary>
    /// <returns>The record body bytes.</returns>
    private static byte[] EncodeCompleted()
    {
        var record = new PersistedIdempotencyRecord("0123456789abcdef0123456789abcdef", "fp", SampleResponse, new DateTime(2026, 7, 1, 12, 0, 0, DateTimeKind.Utc));
        var bytes = new byte[IdempotencyCodec.ComputeEncodedLength(record)];
        IdempotencyCodec.Write(record, bytes);
        return bytes;
    }
}
