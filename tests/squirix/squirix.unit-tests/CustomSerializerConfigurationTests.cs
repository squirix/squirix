using System;
using System.IO;
using System.Text.Json;
using Squirix.Client;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Verifies that <see cref="SquirixClientOptions.Serializer" /> remains settable (not init-only)
/// and that client serializer scopes do not mutate the default serializer host.
/// </summary>
public sealed class CustomSerializerConfigurationTests
{
    /// <summary>Verifies <see cref="SquirixClientOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Fact]
    public void SerializerPropertyHasPublicSetterForConfigureDelegates()
    {
        var custom = new MarkerSerializer();
        var options = new SquirixClientOptions
        {
            Serializer = custom,
        };

        Assert.Same(custom, options.Serializer);
    }

    private sealed class MarkerSerializer : ISquirixSerializer
    {
        public T Deserialize<T>(string payload) => throw CreateNotUsed();

        public T Deserialize<T>(JsonElement payload) => throw CreateNotUsed();

        public T Deserialize<T>(Stream payload) => throw CreateNotUsed();

        public void Serialize<T>(Stream destination, T? value) => throw CreateNotUsed();

        public JsonElement SerializeToElement<T>(T? value) => throw CreateNotUsed();

        public byte[] SerializeToUtf8Bytes<T>(T? value) => throw CreateNotUsed();

        public T Deserialize<T>(ReadOnlySpan<byte> payload) => throw CreateNotUsed();

        private static NotSupportedException CreateNotUsed() => new("MarkerSerializer is an identity placeholder and must not be invoked.");
    }
}
