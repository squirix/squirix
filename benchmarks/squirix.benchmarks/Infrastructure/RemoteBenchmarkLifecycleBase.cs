using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using BenchmarkDotNet.Attributes;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Shared BenchmarkDotNet lifecycle for benchmarks that talk to an in-process node over the remote client SDK.
/// </summary>
[InProcess]
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Base class must remain public for BenchmarkDotNet benchmark classes.")]
public abstract class RemoteBenchmarkLifecycleBase
{
    private BenchmarkCacheSession? _cacheSession;
    private BenchmarkNodeScope? _node;

    /// <summary>
    /// Gets the shared cache opened by <see cref="StartSharedCacheAsync" />.
    /// </summary>
    protected ICache<object?> SharedCache => (_cacheSession ?? throw new InvalidOperationException("Shared cache session was not opened.")).Cache;

    /// <summary>
    /// Connects a client and disposes it before returning.
    /// </summary>
    /// <returns>A task that completes after the client is disposed.</returns>
    protected async Task ConnectAndDisposeClientAsync()
    {
        await StartNodeAsync().ConfigureAwait(false);
        var lease = await OpenClientLeaseAsync().ConfigureAwait(false);
        await lease.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Connects a client, resolves a cache handle, then disposes the client.
    /// </summary>
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
    /// Starts the in-process benchmark node. Safe to call from workload methods before class <c>[GlobalSetup]</c> runs.
    /// </summary>
    /// <returns>A task that completes after the node is started.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to _node which StopNode disposes.")]
    protected async Task StartNodeAsync()
    {
        if (_node is not null)
            return;

        BenchmarkRuntime.EnsureInitialized();
        _node = await BenchmarkNodeScope.StartAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Opens a long-lived client and cache session on the benchmark node.
    /// </summary>
    /// <param name="cacheName">Cache name.</param>
    /// <returns>A task that completes after the shared cache session is opened.</returns>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to _cacheSession which StopSharedCache disposes.")]
    protected async Task StartSharedCacheAsync(string cacheName)
    {
        await StartNodeAsync().ConfigureAwait(false);
        _cacheSession = await BenchmarkCacheSession.OpenAsync(RequireNode(), cacheName, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Stops the in-process benchmark node. Call from each benchmark class <c>[GlobalCleanup]</c>.
    /// </summary>
    /// <returns>A task that completes after the node is stopped.</returns>
    protected async Task StopNodeAsync()
    {
        var node = _node;
        _node = null;
        if (node is not null)
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
        if (session is not null)
            await session.DisposeAsync().ConfigureAwait(false);
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Caller disposes the returned lease.")]
    private async Task<BenchmarkClientLease> OpenClientLeaseAsync() => await RequireNode().OpenClientAsync(CancellationToken.None).ConfigureAwait(false);

    private BenchmarkNodeScope RequireNode() => _node ?? throw new InvalidOperationException("Benchmark node was not started. Global setup did not run.");
}
