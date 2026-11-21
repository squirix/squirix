using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>Checks the compact reply for an ordinary cache lookup.</summary>
public sealed class GetValuePackingTests
{
    /// <summary>Miss, tagged null and an empty byte array each have a distinct representation.</summary>
    [Test]
    public async Task PreservesMissingNullAndEmpty()
    {
        var missing = new GetValueAsyncResponse();
        var nullValue = new GetValueAsyncResponse { Found = true, Value = new CacheValue { NullValue = NullValue.NullValue } };
        var empty = new GetValueAsyncResponse { Found = true, Value = new CacheValue { BinaryValue = ReadOnlyMemory<byte>.Empty } };

        _ = await Assert.That(missing.ToByteArray()).IsEmpty();
        _ = await Assert.That(nullValue.ToByteArray().AsSpan().SequenceEqual<byte>([7])).IsTrue();
        _ = await Assert.That(empty.ToByteArray().AsSpan().SequenceEqual<byte>([31])).IsTrue();
        _ = await Assert.That(GetValueAsyncResponse.Parser.ParseFrom([]).Found).IsFalse();
        _ = await Assert.That(GetValueAsyncResponse.Parser.ParseFrom([7]).Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.NullValue);
        _ = await Assert.That(GetValueAsyncResponse.Parser.ParseFrom([31]).Value!.TakeBinaryValue()).IsEmpty();
    }

    /// <summary>Binary and UTF-8 values consume the rest of the frame.</summary>
    [Test]
    public async Task ReadsTextAndBinaryFromFrameTail()
    {
        var text = new GetValueAsyncResponse { Found = true, Value = new CacheValue { StringValue = "é" } };
        var binary = new GetValueAsyncResponse { Found = true, Value = new CacheValue { BinaryValue = new byte[] { 0, 128, 255 } } };
        byte[] expectedText = [11, 195, 169];
        byte[] expectedBinary = [31, 0, 128, 255];

        _ = await Assert.That(text.ToByteArray().AsSpan().SequenceEqual(expectedText)).IsTrue();
        _ = await Assert.That(binary.ToByteArray().AsSpan().SequenceEqual(expectedBinary)).IsTrue();
        _ = await Assert.That(GetValueAsyncResponse.Parser.ParseFrom(expectedText).Value!.StringValue).IsEqualTo("é");
        _ = await Assert.That(GetValueAsyncResponse.Parser.ParseFrom(expectedBinary).Value!.TakeBinaryValue().AsSpan().SequenceEqual(expectedBinary.AsSpan(1))).IsTrue();
    }

    /// <summary>Reserved header bits, impossible presence and invalid UTF-8 are rejected.</summary>
    /// <param name="hex">The malformed frame body.</param>
    [Test]
    [Arguments("80")]
    [Arguments("04")]
    [Arguments("0BFF")]
    public void RejectsMalformedResponse(string hex) =>
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(Convert.FromHexString(hex), static data => _ = GetValueAsyncResponse.Parser.ParseFrom(data));
}
