using System.IO;
using Squirix.Serialization;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>
/// Verifies that <see cref="SquirixOptions.Serializer" /> remains settable (not init-only)
/// and that client serializer scopes do not mutate the default serializer host.
/// </summary>
public sealed class CustomSerializerConfigurationTests
{
    /// <summary>Mirrors SquirixClient.ConnectAsync options configuration before session creation.</summary>
    [Fact]
    public void ConnectAsyncConfigurePatternAssignsSerializerAfterConstruction()
    {
        var custom = new MarkerSerializer();
        var options = new SquirixOptions();
        ConfigureLikeConnect(options, custom);

        Assert.Same(custom, options.Serializer);
        _ = SerializationProvider.Create(options.Serializer);
    }

    /// <summary>Verifies that a null serializer creates an independent default serializer instance.</summary>
    [Fact]
    public void CreateWithNullSerializerUsesDefault()
    {
        var before = SerializationProvider.Instance;
        var scoped = SerializationProvider.Create();

        _ = Assert.IsType<MetricsDecoratedSerializer>(scoped);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies round-trip fidelity when a custom serializer delegates to the default JSON implementation.</summary>
    [Fact]
    public void CustomSerializerRoundTripsComplexPayload()
    {
        var custom = new SpySerializer(new SystemTextJsonSerializer());
        var scoped = SerializationProvider.Create(custom);

        var payload = new[] { 1, 2, 3 };
        var bytes = scoped.SerializeToUtf8Bytes(payload);
        using var stream = new MemoryStream(bytes);
        var result = scoped.Deserialize<int[]>(stream);

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
        var custom = new SpySerializer(new SystemTextJsonSerializer());
        var opts = new SquirixOptions
        {
            Serializer = custom,
        };

        var before = SerializationProvider.Instance;
        var scoped = SerializationProvider.Create(opts.Serializer);

        var serialized = scoped.SerializeToUtf8Bytes("hello");
        using var stream = new MemoryStream(serialized);
        var deserialized = scoped.Deserialize<string>(stream);

        Assert.Equal("hello", deserialized);
        Assert.Equal(1, custom.SerializeToUtf8BytesCalls);
        Assert.Equal(1, custom.DeserializeStreamCalls);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies that two scoped serializers created in the same process do not replace each other or the default host.</summary>
    [Fact]
    public void ScopedSerializersDoNotCrossAffect()
    {
        var first = new SpySerializer(new SystemTextJsonSerializer());
        var second = new SpySerializer(new SystemTextJsonSerializer());
        var before = SerializationProvider.Instance;

        var firstScoped = SerializationProvider.Create(first);
        var secondScoped = SerializationProvider.Create(second);

        _ = firstScoped.SerializeToUtf8Bytes("first");
        _ = secondScoped.SerializeToUtf8Bytes("second");

        Assert.Equal(1, first.SerializeToUtf8BytesCalls);
        Assert.Equal(1, second.SerializeToUtf8BytesCalls);
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies <see cref="SquirixOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Fact]
    public void SerializerPropertyHasPublicSetterForConfigureDelegates()
    {
        var custom = new MarkerSerializer();
        var options = new SquirixOptions
        {
            Serializer = custom,
        };

        Assert.Same(custom, options.Serializer);
    }

    private static void ConfigureLikeConnect(SquirixOptions options, ISquirixSerializer serializer) => options.Serializer = serializer;
}
