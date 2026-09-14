using System;
using System.Collections.Generic;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Verifies that <see cref="SquirixClientOptions.JsonSerializerContexts" /> flows into the default
/// serializer metadata chain so custom DTOs resolve without a custom serializer.
/// </summary>
[Immutable]
public sealed class JsonSerializerContextRegistrationTests
{
    /// <summary>Verifies options carry application-provided serializer contexts.</summary>
    [Fact]
    public void OptionsCarrySerializerContexts()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);

        Assert.Same(CustomCacheValueJsonContext.Default, Assert.Single(options.JsonSerializerContexts));
    }

    /// <summary>Verifies contexts registered from options resolve custom DTOs through the default serializer.</summary>
    [Fact]
    public void RegisteredContextsResolveCustomDto()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);
        SquirixClient.RegisterSerializerContexts(options);

        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new CustomCacheValue { Count = 7, Name = "dto" };
        var decoded = serializer.Deserialize<CustomCacheValue>(serializer.SerializeToUtf8Bytes(original));

        Assert.Equal("dto", decoded!.Name);
        Assert.Equal(7, decoded.Count);
    }

    /// <summary>Verifies registration keeps the built-in default context first in the chain.</summary>
    [Fact]
    public void RegistrationKeepsDefaultChainIntact()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);
        SquirixClient.RegisterSerializerContexts(options);

        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 5 };
        var utf8 = serializer.SerializeToUtf8Bytes(original);

        Assert.True("""{"value":5}"""u8.SequenceEqual(utf8));
        var decoded = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        Assert.Equal(5, Assert.IsType<Dictionary<string, int>>(decoded)["value"]);
    }
}
