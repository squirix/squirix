using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.Internal;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>
/// Verifies that <see cref="SquirixClientOptions.JsonSerializerContexts" /> flows into the default
/// serializer metadata chain so custom DTOs resolve without a custom serializer.
/// </summary>
[Immutable]
public sealed class JsonSerializerContextRegistrationTests
{
    /// <summary>Verifies options carry application-provided serializer contexts.</summary>
    [Test]
    public async Task OptionsCarrySerializerContexts()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);

        var singleContext = await Assert.That(options.JsonSerializerContexts).HasSingleItem();
        _ = await Assert.That(singleContext).IsSameReferenceAs(CustomCacheValueJsonContext.Default);
    }

    /// <summary>Verifies contexts registered from options resolve custom DTOs through the default serializer.</summary>
    [Test]
    public async Task RegisteredContextsResolveCustomDto()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);
        SquirixClient.RegisterSerializerContexts(options);

        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new CustomCacheValue { Count = 7, Name = "dto" };
        var decoded = serializer.Deserialize<CustomCacheValue>(serializer.SerializeToUtf8Bytes(original));

        _ = await Assert.That(decoded!.Name).IsEqualTo("dto");
        _ = await Assert.That(decoded.Count).IsEqualTo(7);
    }

    /// <summary>Verifies registration keeps the built-in default context first in the chain.</summary>
    [Test]
    public async Task RegistrationKeepsDefaultChainIntact()
    {
        var options = new SquirixClientOptions();
        options.JsonSerializerContexts.Add(CustomCacheValueJsonContext.Default);
        SquirixClient.RegisterSerializerContexts(options);

        var serializer = RemoteClientSessionFactory.CreateSerializer();
        var original = new Dictionary<string, int>(StringComparer.Ordinal) { ["value"] = 5 };
        var utf8 = serializer.SerializeToUtf8Bytes(original);

        _ = await Assert.That("""{"value":5}"""u8.SequenceEqual(utf8)).IsTrue();
        var decoded = serializer.Deserialize<Dictionary<string, int>>(utf8.AsSpan());
        _ = await Assert.That(decoded).IsTypeOf<Dictionary<string, int>>();
        _ = await Assert.That(decoded!["value"]).IsEqualTo(5);
    }
}
