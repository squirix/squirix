using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Xunit;

namespace Squirix.Server.IntegrationTests.Persistence;

/// <summary>
/// Integration tests for the default ephemeral hosting mode.
/// </summary>
public sealed class PersistenceDefaultModeIntegrationTests : IntegrationTestBase
{
    /// <summary>
    /// Ensures default startup does not create WAL, manifest, or snapshot files.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task DefaultStartupDoesNotCreatePersistenceFiles()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node_ephemeral", Url = url } };

        await using var node = await StartNodeAsync(url, peers);
        Assert.False(node.PersistenceEnabled);
        Assert.True(string.IsNullOrWhiteSpace(node.DataDir));
        Assert.Null(node.Services.GetService(typeof(PersistenceOptions)));
    }

    /// <summary>
    /// Ensures cache operations work in the default ephemeral mode.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous operation.</returns>
    [Fact]
    public async Task DefaultModeSupportsCacheOperations()
    {
        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node_ephemeral_ops", Url = url } };

        await using var node = await StartNodeAsync(url, peers);
        var cache = GetCache(node);

        await cache.SetAsync(CacheNames.DefaultNamespace, "ephemeral:key", BuildEntry("value"), DefaultCancellationToken);
        var value = await cache.GetValueAsync(CacheNames.DefaultNamespace, "ephemeral:key", DefaultCancellationToken);
        Assert.Equal("value", value);
    }
}
