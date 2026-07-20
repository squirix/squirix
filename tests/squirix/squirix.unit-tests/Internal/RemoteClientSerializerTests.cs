using System;
using System.Collections.Generic;
using System.IO;
using System.Text.Json;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests.Internal;

/// <summary>Covers metrics-decorated serializer paths used by remote client sessions.</summary>
public sealed class RemoteClientSerializerTests
{
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

    /// <summary>Json failures are recorded and rethrown by the metrics decorator.</summary>
    [Fact]
    public void CreateSerializerRethrowsJsonFailures()
    {
        var serializer = RemoteClientSessionFactory.CreateSerializer();
        _ = Assert.ThrowsAny<JsonException>(() => serializer.Deserialize<Dictionary<string, int>>("{bad"));
    }
}
