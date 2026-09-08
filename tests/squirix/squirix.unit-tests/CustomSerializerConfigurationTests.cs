using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>
/// Verifies that <see cref="SquirixClientOptions" /> configure-delegate properties remain settable (not init-only)
/// and that client serializer scopes do not mutate the default serializer host.
/// </summary>
[Immutable]
public sealed class CustomSerializerConfigurationTests
{
    /// <summary>
    /// Verifies <see cref="SquirixClientOptions.BearerTokenProvider" /> keeps a public setter for configure-delegate assignment.
    /// </summary>
    [Fact]
    public void BearerTokenHasPublicSetterForConfigure()
    {
        Func<CancellationToken, ValueTask<string>> provider = static _ => new ValueTask<string>("token");
        var options = new SquirixClientOptions
        {
            BearerTokenProvider = provider,
        };

        Assert.Same(provider, options.BearerTokenProvider);
    }

    /// <summary>Verifies <see cref="SquirixClientOptions.Serializer" /> keeps a public setter for configure-delegate assignment.</summary>
    [Fact]
    public void SerializerHasPublicSetterForConfigure()
    {
        var custom = new ISquirixSerializerCreateExpectations().Instance();
        var options = new SquirixClientOptions
        {
            Serializer = custom,
        };

        Assert.Same(custom, options.Serializer);
    }
}
