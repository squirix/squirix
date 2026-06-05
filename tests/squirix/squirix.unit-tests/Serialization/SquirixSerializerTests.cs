using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using FakeItEasy;
using Squirix.Serialization;
using Xunit;

namespace Squirix.UnitTests.Serialization;

/// <summary>
/// Tests for the default and configurable serializer infrastructure used by squirix.
/// </summary>
public sealed class SquirixSerializerTests
{
    /// <summary>
    /// Creates a scoped serializer instance and verifies SerializeToUtf8Bytes(string) was called.
    /// </summary>
    [Fact]
    public void CreateWithInstanceUsesProvidedSerializer()
    {
        var serializer = new SystemTextJsonSerializer();
        var customSerializer = A.Fake<ISquirixSerializer>();

        _ = A.CallTo(() => customSerializer.SerializeToUtf8Bytes(A<string>._)).ReturnsLazily((string s) => serializer.SerializeToUtf8Bytes(s));

        var scoped = SerializationProvider.Create(customSerializer);

        _ = scoped.SerializeToUtf8Bytes("ping");

        _ = A.CallTo(() => customSerializer.SerializeToUtf8Bytes("ping")).MustHaveHappenedOnceExactly();
        A.CallTo(() => customSerializer.Serialize(A<Stream>._, A<string>._)).MustNotHaveHappened();
    }

    /// <summary>
    /// Creates a scoped serializer instance and verifies Deserialize(string) is used.
    /// </summary>
    [Fact]
    public void CreateWithOptionsUsesProvidedSerializer()
    {
        const string payload = "{\"VALUE\":42}";
        var serializer = new SystemTextJsonSerializer();
        var custom = A.Fake<ISquirixSerializer>();

        _ = A.CallTo(() => custom.Deserialize<Dictionary<string, int>>(payload)).ReturnsLazily(() => serializer.Deserialize<Dictionary<string, int>>(payload));

        var scoped = SerializationProvider.Create(custom);
        var model = scoped.Deserialize<Dictionary<string, int>>(payload);

        Assert.NotNull(model);
        Assert.True(model.TryGetValue("VALUE", out var value));
        Assert.Equal(42, value);

        _ = A.CallTo(() => custom.Deserialize<Dictionary<string, int>>(payload)).MustHaveHappenedOnceExactly();
        A.CallTo(() => custom.Deserialize<Dictionary<string, int>>(A<Stream>._)).MustNotHaveHappened();
        var called = Fake.GetCalls(custom).Any(static call =>
        {
            var method = call.Method;
            if (method.Name != nameof(ISquirixSerializer.Deserialize))
                return false;

            var parameters = method.GetParameters();
            return parameters.Length == 1 && parameters[0].ParameterType == typeof(ReadOnlySpan<byte>);
        });
        Assert.False(called);
    }

    /// <summary>
    /// Ensures the default serializer host exposes the System.Text.Json implementation.
    /// </summary>
    [Fact]
    public void DefaultInstanceIsSystemTextJson() => Assert.IsType<MetricsDecoratedSerializer>(SerializationProvider.Instance);
}
