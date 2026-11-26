using System;
using System.Buffers;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Protects binary transport ownership and compatibility with the existing disk encoding.</summary>
[Immutable]
public sealed class BinaryValueTests : ServerUnitTestBase
{
    /// <summary>Inbound and outbound mappings copy buffers and preserve invalid UTF-8 and zero bytes.</summary>
    [Test]
    public async Task BinaryMappingsOwnTheirBuffers()
    {
        byte[] source = [0, 255, 128, 34, 0];
        var wire = new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(source)).MapToWire();
        _ = await Assert.That(wire.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        source[0] = 42;
        var decoded = ServerWireMapping.MapCacheValue<byte[]>(wire.Value)!;
        var stored = ServerWireMapping.MapCacheValue<StoredValue>(wire.Value);
        _ = await Assert.That(stored.Kind).IsEqualTo(StoredValue.ValueKind.Binary);
        _ = await Assert.That(decoded.AsSpan().SequenceEqual(new byte[] { 0, 255, 128, 34, 0 })).IsTrue();
        decoded[1] = 42;
        stored.Bytes[2] = 42;
        _ = await Assert.That(wire.Value.BinaryValue.Span[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(wire.Value.BinaryValue.Span[1]).IsEqualTo(byte.MaxValue);
        _ = await Assert.That(wire.Value.BinaryValue.Span[2]).IsEqualTo(byte.CreateChecked(128));
        _ = await Assert.That(stored.Bytes[1]).IsEqualTo(byte.MaxValue);
    }

    /// <summary>Empty binary retains its discriminator across binary serialization.</summary>
    [Test]
    public async Task EmptyBinaryRetainsPresence()
    {
        byte[] empty = [];
        var wire = new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(empty)).MapToWire().Value;
        _ = await Assert.That(wire!.ToByteArray().AsSpan().SequenceEqual(new byte[] { 0x07, 0 })).IsTrue();
        var parsed = CacheValue.Parser.ParseFrom(wire.ToByteArray());
        _ = await Assert.That(parsed.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(ServerWireMapping.MapCacheValue<byte[]>(parsed)!.Length).IsEqualTo(0);
    }

    /// <summary>Binary cannot be coerced into another typed value or confused with an unknown discriminator.</summary>
    [Test]
    public async Task IncompatibleAndUnknownKindsAreRejected()
    {
        var binary = new CacheValue { BinaryValue = ReadOnlyMemory<byte>.Empty };
        var mismatch = NodeExceptionAssert.For<ServerCallException>().Throws(binary, static value => _ = ServerWireMapping.MapCacheValue<string>(value));
        _ = await Assert.That(mismatch.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        byte[] futureKind = [0x08];
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(futureKind, static value => _ = CacheValue.Parser.ParseFrom(value));
    }

    /// <summary>The existing Bytes=3 disk fixture round-trips exactly and does not alias the encoded buffer.</summary>
    [Test]
    public async Task BinaryDiskFixtureIsStableAndOwned()
    {
        byte[] golden = [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 255, 128];
        _ = await Assert.That(CacheEntryCodec.TryRead<StoredValue>(golden, out var entry, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(golden.Length);
        var bytes = StoredValueProbe.Binary(entry!.Value);
        _ = await Assert.That(bytes.AsSpan().SequenceEqual(new byte[] { 0, 255, 128 })).IsTrue();
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(entry)];
        CacheEntryCodec.Write(entry, encoded);
        _ = await Assert.That(encoded.AsSpan().SequenceEqual(golden)).IsTrue();
        golden[^1] = 42;
        _ = await Assert.That(bytes[2]).IsEqualTo(byte.CreateChecked(128));
    }

    /// <summary>Truncated and corrupt binary lengths are rejected without accepting partial entries.</summary>
    [Test]
    public async Task InvalidBinaryDiskLengthsAreRejected()
    {
        byte[] truncated = [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 2, 0, 0, 0, 255];
        _ = await Assert.That(CacheEntryCodec.TryRead<StoredValue>(truncated, out _, out var consumed)).IsFalse();
        _ = await Assert.That(consumed).IsEqualTo(0);
        truncated[13] = 255;
        truncated[14] = 255;
        truncated[15] = 255;
        truncated[16] = 255;
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(truncated)).IsFalse();
    }

    /// <summary>A truncated wire bytes field is rejected before any server value can be materialized.</summary>
    [Test]
    public void TruncatedBinaryWireIsRejected()
    {
        byte[] truncated = [0x07, 2, 0];
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(truncated, static value => _ = CacheValue.Parser.ParseFrom(value));
    }

    /// <summary>Opaque bytes cannot silently pass through the scalar JSON fallback.</summary>
    [Test]
    public async Task BinaryCannotUseScalarJsonFallback()
    {
        var wire = new CacheValue { BinaryValue = new byte[] { 255 } };
        var destination = new ArrayBufferWriter<byte>();
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((wire, destination), static state => CacheValueWire.WriteScalarJson(state.wire, state.destination));
        _ = await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    /// <summary>Canonical remove outcomes omit binary bytes entirely.</summary>
    [Test]
    public async Task BinaryRemoveOutcomeOmitsPayload()
    {
        byte[] expected = [0, 255, 128];
        var decision = ReplicaMutationDecisions.DecideRemove(new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(expected)), new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(decision.Outcome)).IsTrue();
        _ = await Assert.That(decision.Outcome.Length).IsEqualTo(5);
        _ = await Assert.That(decision.Entry).IsNull();
    }

    /// <summary>Memory accounting measures actual binary length and retains the estimate after codec recovery.</summary>
    [Test]
    public async Task BinaryAccountingSurvivesRecovery()
    {
        var estimator = new CacheEntrySizeEstimator<StoredValue>();
        var key = new CacheKey("binary", "key");
        var small = new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(new byte[1]));
        var large = new NodeCacheEntry<StoredValue>(StoredValue.FromBinary(new byte[65536]));
        var difference = estimator.EstimateBytes(key, large, false) - estimator.EstimateBytes(key, small, false);
        _ = await Assert.That(difference).IsEqualTo(65535);
        _ = await Assert.That(estimator.HasUnknownPayloadMagnitude(large, false)).IsFalse();
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(large)];
        CacheEntryCodec.Write(large, encoded);
        _ = await Assert.That(CacheEntryCodec.TryRead<StoredValue>(encoded, out var restored, out _)).IsTrue();
        _ = await Assert.That(estimator.EstimateBytes(key, restored!, false)).IsEqualTo(estimator.EstimateBytes(key, large, false));
    }
}
