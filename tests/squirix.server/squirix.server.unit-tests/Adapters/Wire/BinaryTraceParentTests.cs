using System;
using System.Diagnostics;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>Verifies compact W3C trace identity, literal forwarding and envelope boundaries.</summary>
public sealed class BinaryTraceParentTests
{
    /// <summary>The SDK and an equivalent version-zero traceparent emit the same identifiers and flags.</summary>
    [Test]
    public async Task ActivityContextWritesCompactParent()
    {
        const string parent = "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01";
        var context = new ActivityContext(ActivityTraceId.CreateFromString(parent.AsSpan(3, 32)), ActivitySpanId.CreateFromString(parent.AsSpan(36, 16)), ActivityTraceFlags.Recorded);
        var request = BinaryTracedRequest.Create(in context, string.Empty, new SetAsyncResponse());
        byte[] expected = [1, .. Convert.FromHexString("0123456789abcdef0123456789abcdef0123456789abcdef01"), 0];
        _ = await Assert.That(request.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        _ = await Assert.That(new BinaryTracedRequest { TraceParent = parent }.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
        var parsed = BinaryTracedRequest.Parser.ParseBorrowed(expected);
        _ = await Assert.That(parsed.TraceParent).IsEqualTo(parent);
        _ = await Assert.That(parsed.ToByteArray().AsSpan().SequenceEqual(expected)).IsTrue();
    }

    /// <summary>Nonstandard versions and casing retain their literal spelling.</summary>
    /// <param name="parent">The original parent value.</param>
    [Test]
    [Arguments("01-0123456789abcdef0123456789abcdef-0123456789abcdef-01")]
    [Arguments("00-0123456789Abcdef0123456789abcdef-0123456789abcdef-01")]
    public async Task OtherParentsRetainLiteralText(string parent)
    {
        var bytes = new BinaryTracedRequest { TraceParent = parent }.ToByteArray();
        _ = await Assert.That(bytes[0]).IsEqualTo(byte.MinValue);
        _ = await Assert.That(BinaryTracedRequest.Parser.ParseFrom(bytes).TraceParent).IsEqualTo(parent);
    }

    /// <summary>A truncated compact identifier and unknown selectors fail the envelope.</summary>
    [Test]
    public void RejectsMalformedTraceEnvelope()
    {
        foreach (var bytes in new byte[][] { [2, 0], [1, 0], [1, .. new byte[24]] })
            _ = NodeExceptionAssert.For<InvalidDataException>().Throws(bytes, static frame => _ = BinaryTracedRequest.Parser.ParseFrom(frame));
    }
}
