using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Runtime;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Verifies that test node hosts own runtime resources and service resolution after shutdown is deterministic.</summary>
public sealed class NodeHostCacheLifetimeTests : NodeIntegrationTestBase
{
    /// <summary>Resolving cache APIs through a disposed host fails deterministically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AfterHostDisposedResolvingCacheThrows(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("nodeA", cancellationToken: cancellationToken);
        var host = cluster["nodeA"];
        await cluster.StopNodeAsync("nodeA");
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(host, static value => GetCache(value));
    }

    /// <summary>After the host stops, resolving runtime services from its provider fails deterministically.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResolveThrowsAfterHostDisposal(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("nodeA", cancellationToken: cancellationToken);
        var host = cluster["nodeA"];
        await cluster.StopNodeAsync("nodeA");
        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(host, static value => _ = value.GetRequiredService<ICacheRuntime>());
    }
}
