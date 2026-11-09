using System;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Internal;

/// <summary>Verifies binary reads without changing the typed writer's serializer contract.</summary>
[Immutable]
public sealed class BinaryValueTests
{
    /// <summary>Opaque reads preserve arbitrary bytes and return independently owned arrays without a serializer call.</summary>
    [Test]
    public async Task BinaryReadsOwnBuffersWithoutSerializer()
    {
        var serializer = new ISquirixSerializerCreateExpectations().Instance();
        var wire = new CacheValue { BinaryValue = ByteString.CopyFrom(0, 255, 128, 34, 0) };
        var typed = ProtoEx.Read<byte[]>(wire, serializer)!;
        var untyped = (await Assert.That(ProtoEx.Read<object>(wire, serializer)).IsTypeOf<byte[]>())!;
        _ = await Assert.That(typed.AsSpan().SequenceEqual(wire.BinaryValue.Span)).IsTrue();
        typed[0] = 42;
        untyped[1] = 42;
        _ = await Assert.That(wire.BinaryValue[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(wire.BinaryValue[1]).IsEqualTo(byte.MaxValue);
        _ = await Assert.That(untyped[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(typed[1]).IsEqualTo(byte.MaxValue);
    }

    /// <summary>An empty binary value is present and distinct from null, and is never deserialized as JSON.</summary>
    [Test]
    public async Task EmptyBinaryIsNotNull()
    {
        var serializer = new ISquirixSerializerCreateExpectations().Instance();
        var wire = new CacheValue { BinaryValue = ByteString.Empty };
        var result = ProtoEx.Read<byte[]>(wire, serializer);
        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(result).IsNotNull();
        _ = await Assert.That(result!.Length).IsEqualTo(0);
    }

    /// <summary>Reading an opaque binary value as another CLR type fails explicitly.</summary>
    [Test]
    public async Task BinaryTypeMismatchIsProtocolFailure()
    {
        var serializer = new ISquirixSerializerCreateExpectations().Instance();
        var failure = ExceptionAssert.For<RpcException>().Throws(
            serializer,
            static value => _ = ProtoEx.Read<string>(new CacheValue { BinaryValue = ByteString.Empty }, value));
        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.Internal);
    }

    /// <summary>Existing typed byte-array writes continue to use the configured serializer.</summary>
    [Test]
    public async Task TypedWriterKeepsConfiguredSerializer()
    {
        byte[] serialized = [34, 99, 117, 115, 116, 111, 109, 34];
        var expectations = new ISquirixSerializerCreateExpectations();
        _ = expectations.Setups.SerializeToUtf8Bytes(Arg.Any<byte[]?>()).ReturnValue(serialized);
        var wire = ProtoEx.MapEntryToProto(new CacheEntry<byte[]> { Value = [0, 255] }, expectations.Instance());
        _ = await Assert.That(wire.Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.Value.JsonUtf8.Span.SequenceEqual(serialized)).IsTrue();
    }
}
