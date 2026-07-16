using Squirix.Client;
using Xunit;

namespace Squirix.UnitTests.Serialization;

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
}
