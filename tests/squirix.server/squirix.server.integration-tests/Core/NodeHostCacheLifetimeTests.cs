using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.IntegrationTests.Core;

/// <summary>Verifies that test node hosts own runtime resources and service resolution after shutdown is deterministic.</summary>
public sealed class NodeHostCacheLifetimeTests : IntegrationTestBase
{
    /// <summary>Resolving cache APIs through a disposed host fails deterministically.</summary>
    [Fact]
    public async Task AfterHostDisposedResolvingCacheThrows()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url.AbsoluteUri } };
        var host = await StartNodeAsync(url, peers);
        await host.DisposeAsync();
        var ex = Record.Exception(() => GetCache(host));
        _ = Assert.IsType<ObjectDisposedException>(ex);
    }

    /// <summary>After the host stops, resolving runtime services from its provider fails deterministically.</summary>
    [Fact]
    public async Task AfterHostDisposedServiceProviderThrowsOnResolve()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "nodeA", Url = url.AbsoluteUri } };
        var host = await StartNodeAsync(url, peers);
        await host.DisposeAsync();
        var ex = Record.Exception(ResolveRuntime);
        _ = Assert.IsType<ObjectDisposedException>(ex);
        return;

        void ResolveRuntime()
        {
            _ = host.Services.GetRequiredService<ICacheRuntime>();
        }
    }
}
