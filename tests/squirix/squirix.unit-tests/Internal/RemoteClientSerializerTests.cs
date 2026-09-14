using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers metrics-decorated serializer paths used by remote client sessions.</summary>
[Immutable]
public sealed class RemoteClientSerializerTests
{
    /// <summary>Unhandled exception types bypass the metrics failure filter.</summary>
    [Fact]
    public void BypassesUnhandledExceptionFilter()
    {
        var innerExpectations = new ISquirixSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<InvalidCastException>();
        var serializer = RemoteClientSessionFactory.CreateSerializer(innerExpectations.Instance());
        _ = ExceptionAssert.For<InvalidCastException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Wrapping an already metrics-decorated serializer is idempotent.</summary>
    [Fact]
    public void DoesNotDoubleWrapMetricsDecorator()
    {
        var decorated = RemoteClientSessionFactory.CreateSerializer();
        var again = RemoteClientSessionFactory.CreateSerializer(decorated);
        Assert.Same(decorated, again);
    }

    /// <summary>JSON failures are recorded and rethrown by the metrics decorator.</summary>
    [Fact]
    public void CreateSerializerRethrowsJsonFailures()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        _ = ExceptionAssert.For<JsonException>().ThrowsAny(serializer, static value => value.Deserialize<Dictionary<string, int>>("{bad"));
    }

    /// <summary>NotSupportedException failures are recorded and rethrown.</summary>
    [Fact]
    public void RethrowsNotSupportedFailures()
    {
        var innerExpectations = new ISquirixSerializerCreateExpectations();
        _ = innerExpectations.Setups.SerializeToUtf8Bytes(Arg.Any<string?>()).Throws<NotSupportedException>();
        var serializer = RemoteClientSessionFactory.CreateSerializer(innerExpectations.Instance());
        _ = ExceptionAssert.For<NotSupportedException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Round-trips through the metrics decorator overloads.</summary>
    [Fact]
    public void CreateSerializerRoundTripsPayloads()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 5 };
        var utf8 = serializer.SerializeToUtf8Bytes(original);

        // Golden bytes: the decorated serializer must emit canonical System.Text.Json,
        // not merely something it can read back itself.
        Assert.True("""{"value":5}"""u8.SequenceEqual(utf8));
        var decoded = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        Assert.Equal(5, decoded!["value"]);

        var element = serializer.SerializeToElement(original);
        Assert.Equal("""{"value":5}""", element.GetRawText());
        Assert.Equal(5, serializer.Deserialize<Dictionary<string, int>>(element)!["value"]);

        using var stream = new MemoryStream(utf8);
        Assert.Equal(5, serializer.Deserialize<Dictionary<string, int>>(stream)!["value"]);

        using var destination = new MemoryStream();
        serializer.Serialize(destination, original);
        Assert.True(destination.Length > 0);

        Assert.Equal(5, serializer.Deserialize<Dictionary<string, int>>("""{"value":5}""")!["value"]);
    }

    /// <summary>Metrics decoration can be disabled for custom serializers.</summary>
    [Fact]
    public void WithoutMetricsReturnsInnerInstance()
    {
        var inner = new SystemTextJsonSerializer();
        var serializer = RemoteClientSessionFactory.CreateSerializer(inner, false);
        Assert.Same(inner, serializer);
    }
}
