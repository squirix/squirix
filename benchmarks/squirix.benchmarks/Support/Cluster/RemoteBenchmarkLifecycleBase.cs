using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Support.Client;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Shared BenchmarkDotNet lifecycle for benchmarks that talk to an in-process node over the remote client SDK.</summary>
[InProcess]
public abstract class RemoteBenchmarkLifecycleBase
{
    private BenchmarkCacheSession? _cacheSession;
    private TestCluster<ClusterStartOptions>? _cluster;

    /// <summary>Gets the shared cache opened by <see cref="StartSharedCacheAsync" />.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the shared cache session was not opened.</exception>
    protected ICache<object?> SharedCache => BenchmarkThrowHelper.Required(_cacheSession, "Shared cache session was not opened.").Cache;

    /// <summary>Connects a client and disposes of it before returning.</summary>
    /// <returns>A task that completes after the client is disposed of.</returns>
    protected async Task ConnectAndDisposeClientAsync()
    {
        await StartClusterAsync().ConfigureAwait(false);
        var lease = await OpenClientLeaseAsync().ConfigureAwait(false);
        await lease.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Connects a client, resolves a cache handle, then disposes the client.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <returns>A task that completes after the cache handle is resolved and the client is disposed.</returns>
    protected async Task GetCacheHandleAndDisposeAsync(string cacheName)
    {
        await StartClusterAsync().ConfigureAwait(false);
        var lease = await OpenClientLeaseAsync().ConfigureAwait(false);
        try
        {
            _ = await lease.Client.GetCacheAsync<object?>(cacheName, CancellationToken.None).ConfigureAwait(false);
        }
        finally
        {
            await lease.DisposeAsync().ConfigureAwait(false);
        }
    }

    /// <summary>Starts the in-process benchmark node. Safe to call from workload methods before class <c language="csharp">[GlobalSetup]</c> runs.</summary>
    /// <returns>A task that completes after the node is started.</returns>
    protected async Task StartClusterAsync()
    {
        if (_cluster != null)
            return;

        _cluster = await BenchmarkNodeCluster.StartAsync(cancellationToken: CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Opens a long-lived client and cache session on the benchmark node.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <returns>A task that completes after the shared cache session is opened.</returns>
    protected async Task StartSharedCacheAsync(string cacheName)
    {
        await StartClusterAsync().ConfigureAwait(false);
        _cacheSession = await BenchmarkCacheSession.OpenAsync(RequireCluster().Uri(), cacheName, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Stops the in-process benchmark node. Call from each benchmark class <c language="csharp">[GlobalCleanup]</c>.</summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    protected async Task StopClusterAsync()
    {
        var cluster = _cluster;
        _cluster = null;
        if (cluster != null)
            await cluster.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Disposes the shared cache session opened by <see cref="StartSharedCacheAsync" />.</summary>
    /// <returns>A task that completes after the shared cache session is disposed.</returns>
    protected async Task StopSharedCacheAsync()
    {
        var session = _cacheSession;
        _cacheSession = null;
        if (session != null)
            await session.DisposeAsync().ConfigureAwait(false);
    }

    private Task<BenchmarkClientLease> OpenClientLeaseAsync() => RequireCluster().OpenClientAsync(CancellationToken.None);

    private TestCluster<ClusterStartOptions> RequireCluster() => BenchmarkThrowHelper.Required(_cluster, "Benchmark node was not started. Global setup did not run.");
}
