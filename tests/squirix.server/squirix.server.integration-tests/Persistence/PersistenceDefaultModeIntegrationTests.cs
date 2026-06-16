using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage;
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
        var peers = new[] { new Peer { NodeId = "node_ephemeral_ops", Url = url.AbsoluteUri } };

        await using var node = await StartNodeAsync(url, peers);
        var cache = GetCache(node);

        await cache.SetAsync(CacheNames.DefaultNamespace, "ephemeral:key", BuildEntry("value"), DefaultCancellationToken);
        var value = await cache.GetValueAsync(CacheNames.DefaultNamespace, "ephemeral:key", DefaultCancellationToken);
        Assert.Equal("value", value);
    }

    /// <summary>Ensures default startup does not create WAL, manifest, or snapshot files.</summary>
    [Fact]
    public async Task DefaultStartupDoesNotCreatePersistenceFiles()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node_ephemeral", Url = url.AbsoluteUri } };

        await using var node = await StartNodeAsync(url, peers);
        Assert.False(node.PersistenceEnabled);
        Assert.True(string.IsNullOrWhiteSpace(node.DataDir));
        Assert.Null(node.Services.GetService(typeof(PersistenceOptions)));
    }
}
