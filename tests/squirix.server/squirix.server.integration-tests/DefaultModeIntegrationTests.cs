using System;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Hosting;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration tests for the default ephemeral hosting mode.
/// Uses <see cref="IntegrationSingleNodeFixture"/> to share one server across all tests.
/// </summary>
public sealed class DefaultModeIntegrationTests : NodeIntegrationTestBase, IClassFixture<IntegrationSingleNodeFixture>
{
    private readonly TestNodeHost _node;

    /// <summary>Initializes a new instance of the <see cref="DefaultModeIntegrationTests"/> class.</summary>
    /// <param name="fixture">Shared single-node fixture.</param>
    public DefaultModeIntegrationTests(IntegrationSingleNodeFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        _node = fixture.Node;
    }

    /// <summary>Ensures cache operations work in the default ephemeral mode.</summary>
    [Fact]
    public async Task DefaultModeSupportsCacheOperations()
    {
        var cache = GetCache(_node);

        await cache.SetEntryAsync(IntegrationMutationOpIds.Default, ServerCacheNames.DefaultNamespace, "ephemeral:key", BuildEntry("value"), DefaultCancellationToken);
        var value = await cache.GetValueAsync(ServerCacheNames.DefaultNamespace, "ephemeral:key", DefaultCancellationToken);
        Assert.True(value.Found);
        Assert.Equal("value", value.Value);
    }

    /// <summary>Ensures default startup does not create journal, manifest, or snapshot files.</summary>
    [Fact]
    public void DefaultStartupCreatesNoPersistedFiles()
    {
        Assert.False(_node.PersistenceEnabled);
        Assert.True(string.IsNullOrWhiteSpace(_node.DataDir));
        Assert.Null(_node.Services.GetService(typeof(PersistenceOptions)));
    }
}
