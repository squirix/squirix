using System.Collections.Generic;
using System.IO;
using Squirix.Serialization;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>Tests for the default and configurable serializer infrastructure used by squirix.</summary>
public sealed class SquirixSerializerTests
{
    /// <summary>Creates a scoped serializer instance and verifies SerializeToUtf8Bytes(string) was called.</summary>
    [Fact]
    public void CreateWithInstanceUsesProvidedSerializer()
    {
        var serializer = new SystemTextJsonSerializer();
        var customSerializer = new SpySerializer(serializer);

        var scoped = SerializationProvider.Create(customSerializer);

        _ = scoped.SerializeToUtf8Bytes("ping");

        Assert.Equal(1, customSerializer.SerializeToUtf8BytesCalls);
        Assert.Equal(0, customSerializer.SerializeStreamCalls);
    }

    /// <summary>Creates a scoped serializer instance and verifies Deserialize(string) is used.</summary>
    [Fact]
    public void CreateWithOptionsUsesProvidedSerializer()
    {
        const string payload = "{\"VALUE\":42}";
        var serializer = new SystemTextJsonSerializer();
        var custom = new SpySerializer(serializer);

        var scoped = SerializationProvider.Create(custom);
        var model = scoped.Deserialize<Dictionary<string, int>>(payload);

        Assert.NotNull(model);
        Assert.True(model.TryGetValue("VALUE", out var value));
        Assert.Equal(42, value);

        Assert.Equal(1, custom.DeserializeStringCalls);
        Assert.Equal(0, custom.DeserializeStreamCalls);
    }

    /// <summary>Ensures the default serializer host exposes the System.Text.Json implementation.</summary>
    [Fact]
    public void DefaultInstanceIsSystemTextJson() => Assert.IsType<MetricsDecoratedSerializer>(SerializationProvider.Instance);
}
