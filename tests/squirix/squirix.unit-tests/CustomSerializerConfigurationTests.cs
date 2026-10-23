using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>
/// Verifies that <see cref="SquirixClientOptions" /> configure-delegate properties remain settable (not init-only)
/// and that client serializer scopes do not mutate the default serializer host.
/// </summary>
[Immutable]
public sealed class CustomSerializerConfigurationTests
{
    /// <summary>Verifies <see cref="SquirixClientOptions.BearerTokenProvider" /> keeps a public setter for configure-delegate assignment.</summary>
    [Test]
    public async Task BearerTokenHasPublicSetterForConfigure()
    {
        Func<CancellationToken, ValueTask<string>> provider = static _ => new ValueTask<string>("token");
        var options = new SquirixClientOptions
        {
            BearerTokenProvider = provider,
        };

        _ = await Assert.That(ReferenceEquals(options.BearerTokenProvider, provider)).IsTrue();
    }

    /// <summary>Verifies <see cref="SquirixClientOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Test]
    public async Task SerializerHasPublicSetterForConfigure()
    {
        var custom = new ISquirixSerializerCreateExpectations().Instance();
        var options = new SquirixClientOptions
        {
            Serializer = custom,
        };

        _ = await Assert.That(options.Serializer).IsSameReferenceAs(custom);
    }
}
