using System;
using System.Buffers;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Serialization.Grpc;
using Squirix.Server.Storage.Codecs;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
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
        var wire = new NodeCacheEntry<object?>(source).MapToProto();
        _ = await Assert.That(wire.Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        source[0] = 42;
        var decoded = ServerProtoEx.MapCacheValue<byte[]>(wire.Value)!;
        var untyped = (await Assert.That(ServerProtoEx.MapCacheValue<object>(wire.Value)).IsTypeOf<byte[]>())!;
        _ = await Assert.That(decoded.AsSpan().SequenceEqual(new byte[] { 0, 255, 128, 34, 0 })).IsTrue();
        decoded[1] = 42;
        untyped[2] = 42;
        _ = await Assert.That(wire.Value.BinaryValue[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(wire.Value.BinaryValue[1]).IsEqualTo(byte.MaxValue);
        _ = await Assert.That(wire.Value.BinaryValue[2]).IsEqualTo(byte.CreateChecked(128));
        _ = await Assert.That(untyped[1]).IsEqualTo(byte.MaxValue);
    }

    /// <summary>Empty binary retains its discriminator across protobuf serialization.</summary>
    [Test]
    public async Task EmptyBinaryRetainsPresence()
    {
        byte[] empty = [];
        var wire = new NodeCacheEntry<object?>(empty).MapToProto().Value;
        _ = await Assert.That(wire.ToByteArray().AsSpan().SequenceEqual(new byte[] { 0x3A, 0 })).IsTrue();
        var parsed = CacheValue.Parser.ParseFrom(wire.ToByteArray());
        _ = await Assert.That(parsed.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(ServerProtoEx.MapCacheValue<byte[]>(parsed)!.Length).IsEqualTo(0);
    }

    /// <summary>Binary cannot be coerced into another typed value or confused with an unknown discriminator.</summary>
    [Test]
    public async Task IncompatibleAndUnknownKindsAreRejected()
    {
        var binary = new CacheValue { BinaryValue = ByteString.Empty };
        var mismatch = NodeExceptionAssert.For<RpcException>().Throws(binary, static value => _ = ServerProtoEx.MapCacheValue<string>(value));
        _ = await Assert.That(mismatch.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        byte[] futureKind = [0x42, 0];
        var unknown = CacheValue.Parser.ParseFrom(futureKind);
        var invalid = NodeExceptionAssert.For<RpcException>().Throws(unknown, static value => _ = ServerProtoEx.MapCacheValue<object>(value));
        _ = await Assert.That(invalid.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    /// <summary>The existing Bytes=3 disk fixture round-trips exactly and does not alias the encoded buffer.</summary>
    [Test]
    public async Task BinaryDiskFixtureIsStableAndOwned()
    {
        byte[] golden = [0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 3, 0, 0, 0, 0, 255, 128];
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(golden, out var entry, out var consumed)).IsTrue();
        _ = await Assert.That(consumed).IsEqualTo(golden.Length);
        var bytes = (await Assert.That(entry!.Value).IsTypeOf<byte[]>())!;
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
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(truncated, out _, out var consumed)).IsFalse();
        _ = await Assert.That(consumed).IsEqualTo(0);
        truncated[13] = 255;
        truncated[14] = 255;
        truncated[15] = 255;
        truncated[16] = 255;
        _ = await Assert.That(CacheEntryCodec.IsWellFormed(truncated)).IsFalse();
    }

    /// <summary>A truncated protobuf bytes field is rejected before any server value can be materialized.</summary>
    [Test]
    public void TruncatedBinaryWireIsRejected()
    {
        byte[] truncated = [0x3A, 2, 0];
        _ = NodeExceptionAssert.For<InvalidProtocolBufferException>().Throws(truncated, static value => _ = CacheValue.Parser.ParseFrom(value));
    }

    /// <summary>Opaque bytes cannot silently pass through the scalar JSON fallback.</summary>
    [Test]
    public async Task BinaryCannotUseScalarJsonFallback()
    {
        var wire = new CacheValue { BinaryValue = ByteString.CopyFrom(255) };
        var destination = new ArrayBufferWriter<byte>();
        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((wire, destination), static state => CacheValueWire.WriteScalarJson(state.wire, state.destination));
        _ = await Assert.That(destination.WrittenCount).IsEqualTo(0);
    }

    /// <summary>RPC and replica fingerprints retain binary presence and every payload byte across reserialization.</summary>
    /// <param name="length">The payload length around hashing buffer boundaries.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(512)]
    [Arguments(65536)]
    public async Task BinaryFingerprintsPreserveIdentity(int length)
    {
        var entry = new NodeCacheEntry<object?>(new byte[length]);
        var wire = entry.MapToProto();
        var reparsed = CacheEntryWire.Parser.ParseFrom(wire.ToByteArray());
        var fingerprint = RpcMutationFingerprints.SetEntry("binary", "key", wire);
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("binary", "key", reparsed)).IsEqualTo(fingerprint);
        var replica = ReplicaOperationFingerprints.Set("op", "binary", "key", entry);
        var other = new NodeCacheEntry<object?>(new byte[length + 1]);
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("binary", "key", other.MapToProto())).IsNotEqualTo(fingerprint);
        _ = await Assert.That(replica.AsSpan().SequenceEqual(ReplicaOperationFingerprints.Set("op", "binary", "key", other))).IsFalse();
        var scalar = new NodeCacheEntry<object?>(null).MapToProto();
        _ = await Assert.That(RpcMutationFingerprints.SetEntry("binary", "key", scalar)).IsNotEqualTo(fingerprint);
        if (length > 0)
        {
            var changed = new byte[length];
            changed[^1] = 255;
            var different = new NodeCacheEntry<object?>(changed);
            _ = await Assert.That(RpcMutationFingerprints.SetEntry("binary", "key", different.MapToProto())).IsNotEqualTo(fingerprint);
            _ = await Assert.That(replica.AsSpan().SequenceEqual(ReplicaOperationFingerprints.Set("op", "binary", "key", different))).IsFalse();
        }
    }

    /// <summary>Canonical remove outcomes preserve binary bytes without JSON or metadata changes.</summary>
    [Test]
    public async Task BinaryRemoveOutcomeIsLossless()
    {
        byte[] expected = [0, 255, 128];
        var decision = ReplicaMutationDecisions.DecideRemove(new NodeCacheEntry<object?>(expected), new DateTime(2030, 1, 1, 0, 0, 0, DateTimeKind.Utc));
        var result = ReplicaOutcomeCodec.DecodeRemove(decision.Outcome);
        _ = await Assert.That(result.Removed).IsTrue();
        var value = (await Assert.That(result.Value).IsTypeOf<byte[]>())!;
        _ = await Assert.That(value.AsSpan().SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Memory accounting measures actual binary length and retains the estimate after codec recovery.</summary>
    [Test]
    public async Task BinaryAccountingSurvivesRecovery()
    {
        var estimator = new ObjectCacheEntrySizeEstimator();
        var key = new CacheKey("binary", "key");
        var small = new NodeCacheEntry<object?>(new byte[1]);
        var large = new NodeCacheEntry<object?>(new byte[65536]);
        var difference = estimator.EstimateBytes(key, large, false) - estimator.EstimateBytes(key, small, false);
        _ = await Assert.That(difference).IsEqualTo(65535);
        _ = await Assert.That(estimator.HasUnknownPayloadMagnitude(large, false)).IsFalse();
        var encoded = new byte[CacheEntryCodec.ComputeEncodedLength(large)];
        CacheEntryCodec.Write(large, encoded);
        _ = await Assert.That(CacheEntryCodec.TryRead<object?>(encoded, out var restored, out _)).IsTrue();
        _ = await Assert.That(estimator.EstimateBytes(key, restored!, false)).IsEqualTo(estimator.EstimateBytes(key, large, false));
    }
}
