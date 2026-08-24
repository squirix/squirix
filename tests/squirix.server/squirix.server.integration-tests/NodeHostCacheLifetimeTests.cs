using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
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
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(host, static value => GetCache(value));
    }

    /// <summary>After the host stops, resolving runtime services from its provider fails deterministically.</summary>
    [Fact]
    public async Task ResolveThrowsAfterHostDisposal()
    {
        var uri = GetNextHttpUri();
        var host = await StartNodeAsync(uri, "nodeA");
        await host.DisposeAsync();
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(host, static value => _ = value.Services.GetRequiredService<ICacheRuntime>());
    }
}
