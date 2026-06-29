using System.IO;
using FakeItEasy;
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
        var custom = A.Fake<ISquirixSerializer>();
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
        var custom = CreateDelegatingFake(new SystemTextJsonSerializer());
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
        var custom = CreateDelegatingFake(new SystemTextJsonSerializer());
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
        _ = A.CallTo(() => custom.SerializeToUtf8Bytes("hello")).MustHaveHappenedOnceExactly();
        _ = A.CallTo(() => custom.Deserialize<string>(A<Stream>._)).MustHaveHappenedOnceExactly();
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies that two scoped serializers created in the same process do not replace each other or the default host.</summary>
    [Fact]
    public void ScopedSerializersDoNotCrossAffect()
    {
        var first = CreateDelegatingFake(new SystemTextJsonSerializer());
        var second = CreateDelegatingFake(new SystemTextJsonSerializer());
        var before = SerializationProvider.Instance;

        var firstScoped = SerializationProvider.Create(first);
        var secondScoped = SerializationProvider.Create(second);

        _ = firstScoped.SerializeToUtf8Bytes("first");
        _ = secondScoped.SerializeToUtf8Bytes("second");

        _ = A.CallTo(() => first.SerializeToUtf8Bytes("first")).MustHaveHappenedOnceExactly();
        _ = A.CallTo(() => second.SerializeToUtf8Bytes("second")).MustHaveHappenedOnceExactly();
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies <see cref="SquirixOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Fact]
    public void SerializerPropertyHasPublicSetterForConfigureDelegates()
    {
        var custom = A.Fake<ISquirixSerializer>();
        var options = new SquirixOptions
        {
            Serializer = custom,
        };

        Assert.Same(custom, options.Serializer);
    }

    private static void ConfigureLikeConnect(SquirixOptions options, ISquirixSerializer serializer) => options.Serializer = serializer;

    private static ISquirixSerializer CreateDelegatingFake(SystemTextJsonSerializer inner)
    {
        var fake = A.Fake<ISquirixSerializer>();

        _ = A.CallTo(() => fake.SerializeToUtf8Bytes(A<string>._)).ReturnsLazily((string value) => inner.SerializeToUtf8Bytes(value));
        _ = A.CallTo(() => fake.SerializeToUtf8Bytes(A<int[]>._)).ReturnsLazily((int[] value) => inner.SerializeToUtf8Bytes(value));
        _ = A.CallTo(() => fake.Deserialize<string>(A<Stream>._)).ReturnsLazily((Stream stream) => inner.Deserialize<string>(stream));
        _ = A.CallTo(() => fake.Deserialize<int[]>(A<Stream>._)).ReturnsLazily((Stream stream) => inner.Deserialize<int[]>(stream));

        return fake;
    }
}
