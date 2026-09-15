using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using System.Threading.Tasks;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers metrics-decorated serializer paths used by remote client sessions.</summary>
[Immutable]
public sealed class RemoteClientSerializerTests
{
    /// <summary>Unhandled exception types bypass the metrics failure filter.</summary>
    [Test]
    public void BypassesUnhandledExceptionFilter()
    {
        var innerExpectations = new ISquirixSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<InvalidCastException>();
        var serializer = RemoteClientSessionFactory.CreateSerializer(innerExpectations.Instance());
        _ = ExceptionAssert.For<InvalidCastException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>JSON failures are recorded and rethrown by the metrics decorator.</summary>
    [Test]
    public void CreateSerializerRethrowsJsonFailures()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        _ = ExceptionAssert.For<JsonException>().ThrowsAny(serializer, static value => value.Deserialize<Dictionary<string, int>>("{bad"));
    }

    /// <summary>Round-trips through the metrics decorator overloads.</summary>
    [Test]
    public async Task CreateSerializerRoundTripsPayloads()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 5 };
        var utf8 = serializer.SerializeToUtf8Bytes(original);

        // Golden bytes: the decorated serializer must emit canonical System.Text.Json,
        // not merely something it can read back itself.
        _ = await Assert.That("""{"value":5}"""u8.SequenceEqual(utf8)).IsTrue();
        var decoded = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        _ = await Assert.That(decoded!["value"]).IsEqualTo(5);

        var element = serializer.SerializeToElement(original);
        _ = await Assert.That(element.GetRawText()).IsEqualTo("""{"value":5}""");
        _ = await Assert.That(serializer.Deserialize<Dictionary<string, int>>(element)!["value"]).IsEqualTo(5);

        await using var stream = new MemoryStream(utf8);
        _ = await Assert.That(serializer.Deserialize<Dictionary<string, int>>(stream)!["value"]).IsEqualTo(5);

        await using var destination = new MemoryStream();
        serializer.Serialize(destination, original);
        _ = await Assert.That(destination.Length > 0).IsTrue();

        _ = await Assert.That(serializer.Deserialize<Dictionary<string, int>>("""{"value":5}""")!["value"]).IsEqualTo(5);
    }

    /// <summary>Wrapping an already metrics-decorated serializer is idempotent.</summary>
    [Test]
    public async Task DoesNotDoubleWrapMetricsDecorator()
    {
        var decorated = RemoteClientSessionFactory.CreateSerializer();
        var again = RemoteClientSessionFactory.CreateSerializer(decorated);
        _ = await Assert.That(again).IsSameReferenceAs(decorated);
    }

    /// <summary>NotSupportedException failures are recorded and rethrown.</summary>
    [Test]
    public void RethrowsNotSupportedFailures()
    {
        var innerExpectations = new ISquirixSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<NotSupportedException>();
        var serializer = RemoteClientSessionFactory.CreateSerializer(innerExpectations.Instance());
        _ = ExceptionAssert.For<NotSupportedException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Metrics decoration can be disabled for custom serializers.</summary>
    [Test]
    public async Task WithoutMetricsReturnsInnerInstance()
    {
        var inner = new SystemTextJsonSerializer();
        var serializer = RemoteClientSessionFactory.CreateSerializer(inner, false);
        _ = await Assert.That(serializer).IsSameReferenceAs(inner);
    }
}
