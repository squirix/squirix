using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Squirix.Internal;
using Squirix.TestKit;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers metrics-decorated serializer paths used by remote client sessions.</summary>
public sealed class RemoteClientSerializerTests
{
    /// <summary>Unhandled exception types bypass the metrics failure filter.</summary>
    [Fact]
    public void CreateSerializerBypassesUnhandledExceptionFilter()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer(new ThrowingSerializer(new InvalidCastException("nope")));
        _ = ExceptionAssert.For<InvalidCastException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Wrapping an already metrics-decorated serializer is idempotent.</summary>
    [Fact]
    public void CreateSerializerDoesNotDoubleWrapMetricsDecorator()
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
    public void CreateSerializerRethrowsNotSupportedFailures()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer(new ThrowingSerializer(new NotSupportedException("boom")));
        _ = ExceptionAssert.For<NotSupportedException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Round-trips through the metrics decorator overloads.</summary>
    [Fact]
    public void CreateSerializerRoundTripsPayloads()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 5 };
        var utf8 = serializer.SerializeToUtf8Bytes(original);
        var decoded = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        Assert.Equal(5, decoded!["value"]);

        var element = serializer.SerializeToElement(original);
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
    public void CreateSerializerWithoutMetricsReturnsInnerInstance()
    {
        var inner = new SystemTextJsonSerializer();
        var serializer = RemoteClientSessionFactory.CreateSerializer(inner, false);
        Assert.Same(inner, serializer);
    }

    private sealed class ThrowingSerializer : ISquirixSerializer
    {
        private readonly Exception _exception;

        internal ThrowingSerializer(Exception exception)
        {
            _exception = exception;
        }

        public T Deserialize<T>(string payload) => throw _exception;

        public T Deserialize<T>(JsonElement payload) => throw _exception;

        public T Deserialize<T>(ReadOnlySpan<byte> payload) => throw _exception;

        public T Deserialize<T>(Stream payload) => throw _exception;

        public void Serialize<T>(Stream destination, T? value) => throw _exception;

        public JsonElement SerializeToElement<T>(T? value) => throw _exception;

        public byte[] SerializeToUtf8Bytes<T>(T? value) => throw _exception;
    }
}
