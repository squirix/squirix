using System;
using System.IO;
using System.Reflection;
using System.Text.Json;
using Squirix.Serialization;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>
/// Verifies that <see cref="SquirixOptions.Serializer" /> remains settable (not init-only)
/// and that client serializer scopes do not mutate the default serializer host.
/// </summary>
public sealed class CustomSerializerConfigurationTests
{
    /// <summary>
    /// Mirrors SquirixClient.ConnectAsync options configuration before session creation.
    /// </summary>
    [Fact]
    public void ConnectAsyncConfigurePatternAssignsSerializerAfterConstruction()
    {
        var custom = new CallTrackingSerializer();
        var options = new SquirixOptions();
        ConfigureLikeConnectAsync(options, custom);

        Assert.Same(custom, options.Serializer);
        _ = SerializationProvider.Create(options.Serializer);
    }

    /// <summary>
    /// Verifies that a null serializer creates an independent default serializer instance.
    /// </summary>
    [Fact]
    public void CreateWithNullSerializerUsesDefault()
    {
        var before = SerializationProvider.Instance;
        var scoped = SerializationProvider.Create();

        _ = Assert.IsType<MetricsDecoratedSerializer>(scoped);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>
    /// Verifies round-trip fidelity when a custom serializer delegates to <see cref="JsonSerializer" />.
    /// </summary>
    [Fact]
    public void CustomSerializerRoundTripsComplexPayload()
    {
        var custom = new CallTrackingSerializer();
        var scoped = SerializationProvider.Create(custom);

        var payload = new[] { 1, 2, 3 };
        var bytes = scoped.SerializeToUtf8Bytes(payload);
        var result = scoped.Deserialize<int[]>(bytes);

        Assert.NotNull(result);
        Assert.Equal(payload, result);
    }

    /// <summary>
    /// The SquirixClient.ConnectAsync delegate pattern constructs <see cref="SquirixOptions" /> first, then passes it to a caller-provided
    /// delegate that sets <see cref="SquirixOptions.Serializer" />. This test verifies a custom serializer
    /// configured this way can be converted into a scoped serializer without mutating the default host.
    /// </summary>
    [Fact]
    public void PostConstructionSerializerAssignmentCreatesScopedSerializer()
    {
        var custom = new CallTrackingSerializer();
        var opts = new SquirixOptions
        {
            Serializer = custom,
        };

        var before = SerializationProvider.Instance;
        var scoped = SerializationProvider.Create(opts.Serializer);

        var serialized = scoped.SerializeToUtf8Bytes("hello");
        var deserialized = scoped.Deserialize<string>(serialized);

        Assert.Equal("hello", deserialized);
        Assert.True(custom.SerializeCalled);
        Assert.True(custom.DeserializeCalled);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>
    /// Verifies that two scoped serializers created in the same process do not replace each other or the default host.
    /// </summary>
    [Fact]
    public void ScopedSerializersDoNotCrossAffect()
    {
        var first = new CallTrackingSerializer();
        var second = new CallTrackingSerializer();
        var before = SerializationProvider.Instance;

        var firstScoped = SerializationProvider.Create(first);
        var secondScoped = SerializationProvider.Create(second);

        _ = firstScoped.SerializeToUtf8Bytes("first");
        _ = secondScoped.SerializeToUtf8Bytes("second");

        Assert.Equal(1, first.SerializeCallCount);
        Assert.Equal(1, second.SerializeCallCount);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>
    /// Verifies <see cref="SquirixOptions.Serializer" /> keeps a public setter for configure-delegate assignment.
    /// </summary>
    [Fact]
    public void SerializerPropertyHasPublicSetterForConfigureDelegates()
    {
        var property = typeof(SquirixOptions).GetProperty(nameof(SquirixOptions.Serializer), BindingFlags.Public | BindingFlags.Instance);

        Assert.NotNull(property);
        Assert.True(property.CanWrite);
        Assert.NotNull(property.SetMethod);
        Assert.True(property.SetMethod.IsPublic);
    }

    private static void ConfigureLikeConnectAsync(SquirixOptions options, ISquirixSerializer serializer) => options.Serializer = serializer;

    private sealed class CallTrackingSerializer : ISquirixSerializer
    {
        public bool DeserializeCalled { get; private set; }

        public int SerializeCallCount { get; private set; }

        public bool SerializeCalled { get; private set; }

        public T? Deserialize<T>(string payload)
        {
            DeserializeCalled = true;
            return JsonSerializer.Deserialize<T>(payload);
        }

        public T? Deserialize<T>(JsonElement payload)
        {
            DeserializeCalled = true;
            return payload.Deserialize<T>();
        }

        public T? Deserialize<T>(ReadOnlySpan<byte> payload)
        {
            DeserializeCalled = true;
            return JsonSerializer.Deserialize<T>(payload);
        }

        public T? Deserialize<T>(Stream payload)
        {
            DeserializeCalled = true;
            return JsonSerializer.Deserialize<T>(payload);
        }

        public void Serialize<T>(Stream destination, T? value)
        {
            SerializeCalled = true;
            SerializeCallCount++;
            JsonSerializer.Serialize(destination, value);
        }

        public JsonElement SerializeToElement<T>(T? value)
        {
            SerializeCalled = true;
            SerializeCallCount++;
            return JsonSerializer.SerializeToElement(value);
        }

        public byte[] SerializeToUtf8Bytes<T>(T? value)
        {
            SerializeCalled = true;
            SerializeCallCount++;
            return JsonSerializer.SerializeToUtf8Bytes(value);
        }
    }
}
