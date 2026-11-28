using System;
using System.Runtime.InteropServices;
using System.Text.Json.Serialization.Metadata;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using Squirix.Transport;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.UnitTests.Internal;

/// <summary>Verifies binary reads without changing the typed writer's serializer contract.</summary>
[Immutable]
public sealed class BinaryValueTests
{
    /// <summary>Opaque reads preserve arbitrary bytes and return independently owned arrays without a serializer call.</summary>
    [Test]
    public async Task BinaryReadsOwnBuffersWithoutSerializer()
    {
        var serializer = ISquirixSerializer.Mock(MockBehavior.Strict).Object;
        var wire = new CacheValue { BinaryValue = new byte[] { 0, 255, 128, 34, 0 } };
        var typed = WireMapping.Read<byte[]>(wire, serializer)!;
        var untyped = (await Assert.That(WireMapping.Read<object>(wire, serializer)).IsTypeOf<byte[]>())!;
        _ = await Assert.That(typed.AsSpan().SequenceEqual(wire.BinaryValue.Span)).IsTrue();
        typed[0] = 42;
        untyped[1] = 42;
        _ = await Assert.That(wire.BinaryValue.Span[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(wire.BinaryValue.Span[1]).IsEqualTo(byte.MaxValue);
        _ = await Assert.That(untyped[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(typed[1]).IsEqualTo(byte.MaxValue);
    }

    /// <summary>Typed and object reads take the owned array of a native result after the input frame becomes reusable.</summary>
    /// <param name="typed">Whether the caller requested a byte array directly.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task NativeBinaryReadTransfersDecodedArray(bool typed)
    {
        var serializer = ISquirixSerializer.Mock(MockBehavior.Strict).Object;
        byte[] frame = [31, 19, 42];
        var wire = GetValueAsyncResponse.Parser.ParseFrom(frame).Value!;
        _ = await Assert.That(MemoryMarshal.TryGetArray(wire.BinaryValue, out var original)).IsTrue();
        var bytes = typed ? WireMapping.Read<byte[]>(wire, serializer)! : (await Assert.That(WireMapping.Read<object>(wire, serializer)).IsTypeOf<byte[]>())!;
        _ = await Assert.That(ReferenceEquals(original.Array, bytes)).IsTrue();
        frame[1] = 0;
        _ = await Assert.That(bytes[0]).IsEqualTo(byte.CreateChecked(19));
        bytes[1] = 0;
        _ = await Assert.That(frame[^1]).IsEqualTo(byte.CreateChecked(42));
    }

    /// <summary>Borrowing a native frame keeps its binary bytes isolated from a caller's mutable result.</summary>
    [Test]
    public async Task BorrowedBinaryReadCopiesOnEachMapping()
    {
        var serializer = ISquirixSerializer.Mock(MockBehavior.Strict).Object;
        byte[] frame = [31, 42];
        var wire = GetValueAsyncResponse.Parser.ParseBorrowed(frame).Value!;
        var first = WireMapping.Read<byte[]>(wire, serializer)!;
        first[0] = 0;
        _ = await Assert.That(WireMapping.Read<byte[]>(wire, serializer)![0]).IsEqualTo(byte.CreateChecked(42));
        _ = await Assert.That(frame[^1]).IsEqualTo(byte.CreateChecked(42));
    }

    /// <summary>An empty binary value is present and distinct from null, and is never deserialized as JSON.</summary>
    [Test]
    public async Task EmptyBinaryIsNotNull()
    {
        var serializer = ISquirixSerializer.Mock(MockBehavior.Strict).Object;
        var wire = new CacheValue { BinaryValue = ReadOnlyMemory<byte>.Empty };
        var result = WireMapping.Read<byte[]>(wire, serializer);
        _ = await Assert.That(wire.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(result).IsNotNull();
        _ = await Assert.That(result!.Length).IsEqualTo(0);
    }

    /// <summary>Reading an opaque binary value as another CLR type fails explicitly.</summary>
    [Test]
    public async Task BinaryTypeMismatchIsProtocolFailure()
    {
        var serializer = ISquirixSerializer.Mock(MockBehavior.Strict).Object;
        var failure = ExceptionAssert.For<SquirixTransportException>().Throws(
            serializer,
            static value => _ = WireMapping.Read<string>(new CacheValue { BinaryValue = ReadOnlyMemory<byte>.Empty }, value));
        _ = await Assert.That(failure.StatusCode).IsEqualTo(CacheTransportStatusCode.Internal);
    }

    /// <summary>Existing typed byte-array writes continue to use the configured serializer.</summary>
    [Test]
    public async Task TypedWriterKeepsConfiguredSerializer()
    {
        byte[] serialized = [34, 99, 117, 115, 116, 111, 109, 34];
        var expectations = ISquirixSerializer.Mock(MockBehavior.Strict);
        _ = expectations.SerializeToUtf8Bytes<byte[]>(Arg.Any<byte[]?>(), default(JsonTypeInfo<byte[]>?)).Returns(serialized);
        var wire = WireMapping.CreateWireEntry<byte[]>([0, 255], expectations.Object);
        _ = await Assert.That(wire.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.JsonUtf8);
        _ = await Assert.That(wire.Value.JsonUtf8.Span.SequenceEqual(serialized)).IsTrue();
    }
}
