using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Observability;

/// <summary>Covers metrics decorator paths around <see cref="ServerMetricsSerializer" />.</summary>
public sealed class ServerMetricsSerializerTests : ServerUnitTestBase
{
    /// <summary>Json failures are recorded and rethrown.</summary>
    [Fact]
    public void DeserializeInvalidJsonRethrowsJsonException()
    {
        var serializer = new ServerMetricsSerializer(new ServerJsonSerializer());
        _ = NodeExceptionAssert.For<JsonException>().ThrowsAny(serializer, static value => value.Deserialize<Dictionary<string, int>>("{not-json"));
    }

    /// <summary>Successful serialize/deserialize overloads record without throwing.</summary>
    [Fact]
    public void RoundTripOverloadsSucceed()
    {
        var serializer = new ServerMetricsSerializer(new ServerJsonSerializer());
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 7 };
        const string payload = """{"value":7}""";

        var fromString = serializer.Deserialize<Dictionary<string, int>>(payload);
        Assert.NotNull(fromString);
        Assert.Equal(7, fromString["value"]);

        var element = serializer.SerializeToElement(original);
        var fromElement = serializer.Deserialize<Dictionary<string, int>>(element);
        Assert.Equal(7, fromElement!["value"]);

        var utf8 = serializer.SerializeToUtf8Bytes(original);
        var fromBytes = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        Assert.Equal(7, fromBytes!["value"]);

        using var stream = new MemoryStream(utf8);
        var fromStream = serializer.Deserialize<Dictionary<string, int>>(stream);
        Assert.Equal(7, fromStream!["value"]);

        using var destination = new MemoryStream();
        serializer.Serialize(destination, original);
        Assert.True(destination.Length > 0);
    }

    /// <summary>Inner NotSupportedException failures are recorded and rethrown.</summary>
    [Fact]
    public void SerializeFailureFromInnerIsRethrown()
    {
        var serializer = new ServerMetricsSerializer(new ThrowingSerializer(new NotSupportedException("boom")));
        _ = NodeExceptionAssert.For<NotSupportedException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>IOException failures are recorded and rethrown.</summary>
    [Fact]
    public void SerializeIoFailureFromInnerIsRethrown()
    {
        var serializer = new ServerMetricsSerializer(new ThrowingSerializer(new IOException("io")));
        _ = NodeExceptionAssert.For<IOException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    /// <summary>Unhandled exception types are not filtered by the metrics decorator.</summary>
    [Fact]
    public void UnhandledExceptionBypassesFailureFilter()
    {
        var serializer = new ServerMetricsSerializer(new ThrowingSerializer(new InvalidCastException("nope")));
        _ = NodeExceptionAssert.For<InvalidCastException>().Throws(serializer, static value => value.SerializeToUtf8Bytes("x"));
    }

    private sealed class ThrowingSerializer : IServerSerializer
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
