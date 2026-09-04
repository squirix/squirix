using System;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;
using Squirix.Benchmarks.Support.Client;

namespace Squirix.Benchmarks.Support.Cluster;

/// <summary>Shared BenchmarkDotNet lifecycle for benchmarks that talk to an in-process node over the remote client SDK.</summary>
[InProcess]
public abstract class RemoteBenchmarkLifecycleBase
{
    private BenchmarkCacheSession? _cacheSession;
    private BenchmarkNodeScope? _node;

    /// <summary>
    /// Gets the shared cache opened by <see cref="StartSharedCacheAsync" />.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown when the shared cache session was not opened.</exception>
    protected ICache<object?> SharedCache => BenchmarkThrowHelper.Required(_cacheSession, "Shared cache session was not opened.").Cache;

    /// <summary>Connects a client and disposes it before returning.</summary>
    /// <returns>A task that completes after the client is disposed.</returns>
    protected async Task ConnectAndDisposeClientAsync()
    {
        await StartNodeAsync().ConfigureAwait(false);
        var lease = await OpenClientLeaseAsync().ConfigureAwait(false);
        await lease.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>Connects a client, resolves a cache handle, then disposes the client.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <returns>A task that completes after the cache handle is resolved and the client is disposed.</returns>
    protected async Task GetCacheHandleAndDisposeAsync(string cacheName)
    {
        await StartNodeAsync().ConfigureAwait(false);
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

    /// <summary>
    /// Starts the in-process benchmark node. Safe to call from workload methods before class <c language="csharp">[GlobalSetup]</c> runs.
    /// </summary>
    /// <returns>A task that completes after the node is started.</returns>
    protected async Task StartNodeAsync()
    {
        if (_node != null)
            return;

        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Opens a long-lived client and cache session on the benchmark node.</summary>
    /// <param name="cacheName">Cache name.</param>
    /// <returns>A task that completes after the shared cache session is opened.</returns>
    protected async Task StartSharedCacheAsync(string cacheName)
    {
        await StartNodeAsync().ConfigureAwait(false);
        _cacheSession = await BenchmarkCacheSession.OpenAsync(RequireNode().Uri, cacheName, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the in-process benchmark node. Call from each benchmark class <c language="csharp">[GlobalCleanup]</c>.
    /// </summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    protected async Task StopNodeAsync()
    {
        var node = _node;
        _node = null;
        if (node != null)
            await node.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Disposes the shared cache session opened by <see cref="StartSharedCacheAsync" />.
    /// </summary>
    /// <returns>A task that completes after the shared cache session is disposed.</returns>
    protected async Task StopSharedCacheAsync()
    {
        var session = _cacheSession;
        _cacheSession = null;
        if (session != null)
            await session.DisposeAsync().ConfigureAwait(false);
    }

    private Task<BenchmarkClientLease> OpenClientLeaseAsync() => RequireNode().OpenClientAsync(CancellationToken.None);

    private BenchmarkNodeScope RequireNode() => BenchmarkThrowHelper.Required(_node, "Benchmark node was not started. Global setup did not run.");
}
