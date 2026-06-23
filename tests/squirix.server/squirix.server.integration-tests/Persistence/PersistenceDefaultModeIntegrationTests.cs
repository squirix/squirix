using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence;

/// <summary>Integration tests for the default ephemeral hosting mode.</summary>
public sealed class PersistenceDefaultModeIntegrationTests : IntegrationTestBase
{
    /// <summary>Ensures cache operations work in the default ephemeral mode.</summary>
    [Fact]
    public async Task DefaultModeSupportsCacheOperations()
    {
        var url = GetNextHttpUri();
        await using var node = await StartNodeAsync(url, "node_ephemeral_ops");
        var cache = GetCache(node);

        await cache.SetEntryAsync(TestOperationIds.Default, CacheNames.DefaultNamespace, "ephemeral:key", BuildEntry("value"), DefaultCancellationToken);
        var value = await cache.GetValueAsync(CacheNames.DefaultNamespace, "ephemeral:key", DefaultCancellationToken);
        Assert.True(value.Found);
        Assert.Equal("value", value.Value);
    }

    /// <summary>Ensures default startup does not create journal, manifest, or snapshot files.</summary>
    [Fact]
    public async Task DefaultStartupDoesNotCreatePersistenceFiles()
    {
        var url = GetNextHttpUri();
        await using var node = await StartNodeAsync(url, "node_ephemeral");
        Assert.False(node.PersistenceEnabled);
        Assert.True(string.IsNullOrWhiteSpace(node.DataDir));
        Assert.Null(node.Services.GetService(typeof(PersistenceOptions)));
    }
}
