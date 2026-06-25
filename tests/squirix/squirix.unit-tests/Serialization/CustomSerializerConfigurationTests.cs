using System;
using System.IO;
using System.Reflection;
using FakeItEasy;
using FakeItEasy.Core;
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
        _ = A.CallTo(custom).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToUtf8Bytes) && call.Method.IsGenericMethod).MustHaveHappenedOnceExactly();
        _ = A.CallTo(custom).Where(static call => call.Method.Name == nameof(ISquirixSerializer.Deserialize) && call.Method.IsGenericMethod).MustHaveHappenedOnceExactly();
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

        _ = A.CallTo(first).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToUtf8Bytes) && call.Method.IsGenericMethod).MustHaveHappenedOnceExactly();
        _ = A.CallTo(second).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToUtf8Bytes) && call.Method.IsGenericMethod).MustHaveHappenedOnceExactly();
        Assert.Same(before, SerializationProvider.Instance);
    }

    /// <summary>Verifies <see cref="SquirixOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Fact]
    public void SerializerPropertyHasPublicSetterForConfigureDelegates()
    {
        var property = typeof(SquirixOptions).GetProperty(nameof(SquirixOptions.Serializer), BindingFlags.Public | BindingFlags.Instance);

        Assert.NotNull(property);
        Assert.True(property.CanWrite);
        Assert.NotNull(property.SetMethod);
        Assert.True(property.SetMethod.IsPublic);
    }

    private static void ConfigureLikeConnect(SquirixOptions options, ISquirixSerializer serializer) => options.Serializer = serializer;

    private static ISquirixSerializer CreateDelegatingFake(SystemTextJsonSerializer inner)
    {
        var fake = A.Fake<ISquirixSerializer>();

        _ = A.CallTo(fake).Where(static call => call.Method.Name == nameof(ISquirixSerializer.SerializeToUtf8Bytes) && call.Method.IsGenericMethod).WithReturnType<byte[]>()
             .ReturnsLazily(call => InvokeSerializeToUtf8Bytes(inner, call));

        _ = A.CallTo(fake).Where(static call => IsStreamDeserialize<string>(call)).WithReturnType<string>()
             .ReturnsLazily(call => DeserializeFromStream(inner, call, static (serializer, stream) => serializer.Deserialize<string>(stream)));

        _ = A.CallTo(fake).Where(static call => IsStreamDeserialize<int[]>(call)).WithReturnType<int[]>()
             .ReturnsLazily(call => DeserializeFromStream(inner, call, static (serializer, stream) => serializer.Deserialize<int[]>(stream)));

        return fake;
    }

    private static T DeserializeFromStream<T>(SystemTextJsonSerializer inner, IFakeObjectCall call, Func<SystemTextJsonSerializer, Stream, T?> deserialize)
    {
        if (call.Arguments[0] is not Stream stream)
            throw new InvalidOperationException("Expected Stream argument.");

        return deserialize(inner, stream) ?? throw new InvalidOperationException($"Deserialization returned null for {typeof(T).Name}.");
    }

    private static bool IsStreamDeserialize<T>(IFakeObjectCall call) => string.Equals(call.Method.Name, nameof(ISquirixSerializer.Deserialize), StringComparison.Ordinal) &&
                                                                        call.Method.IsGenericMethod && call.Method.GetGenericArguments()[0] == typeof(T) &&
                                                                        call.Method.GetParameters()[0].ParameterType == typeof(Stream);

    private static byte[] InvokeSerializeToUtf8Bytes(SystemTextJsonSerializer inner, IFakeObjectCall call)
    {
        var valueType = call.Method.GetGenericArguments()[0];
        var method = FindGenericMethod(nameof(SystemTextJsonSerializer.SerializeToUtf8Bytes), 1);
        var result = method.MakeGenericMethod(valueType).Invoke(inner, [call.Arguments[0]]);
        return result as byte[] ?? throw new InvalidOperationException($"Expected byte[], got {result?.GetType().Name ?? "null"}.");
    }

    private static MethodInfo FindGenericMethod(string methodName, int parameterCount)
    {
        foreach (var method in typeof(SystemTextJsonSerializer).GetMethods(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!string.Equals(method.Name, methodName, StringComparison.Ordinal) || !method.IsGenericMethodDefinition)
                continue;

            var parameters = method.GetParameters();
            if (parameters.Length != parameterCount)
                continue;

            return method;
        }

        throw new InvalidOperationException($"Generic method '{methodName}' not found.");
    }
}
