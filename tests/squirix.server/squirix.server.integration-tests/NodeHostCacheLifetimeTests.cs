using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies that test node hosts own runtime resources and service resolution after shutdown is deterministic.</summary>
public sealed class NodeHostCacheLifetimeTests : NodeIntegrationTestBase
{
    /// <summary>Resolving cache APIs through a disposed host fails deterministically.</summary>
    [Fact]
    public async Task AfterHostDisposedResolvingCacheThrows()
    {
        var uri = GetNextHttpUri();
        var host = await StartNodeAsync(uri, "nodeA");
        await host.DisposeAsync();
        var ex = Record.Exception(() => GetCache(host));
        _ = Assert.IsType<ObjectDisposedException>(ex);
    }

    /// <summary>After the host stops, resolving runtime services from its provider fails deterministically.</summary>
    [Fact]
    public async Task AfterHostDisposedServiceProviderThrowsOnResolve()
    {
        var uri = GetNextHttpUri();
        var host = await StartNodeAsync(uri, "nodeA");
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
