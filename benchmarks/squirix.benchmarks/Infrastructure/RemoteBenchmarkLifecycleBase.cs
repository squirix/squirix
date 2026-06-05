using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using BenchmarkDotNet.Attributes;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Shared BenchmarkDotNet lifecycle for benchmarks that talk to an in-process node over the remote client SDK.
/// </summary>
[InProcess]
public abstract class RemoteBenchmarkLifecycleBase
{
    private BenchmarkCacheSession? _cacheSession;
    private BenchmarkNodeScope? _node;

    /// <summary>
    /// Gets the shared cache opened by <see cref="StartSharedCache" />.
    /// </summary>
    protected ICache<object?> SharedCache =>
        (_cacheSession ?? throw new InvalidOperationException("Shared cache session was not opened.")).Cache;

    /// <summary>
    /// Starts the in-process benchmark node. Safe to call from workload methods before class <c>[GlobalSetup]</c> runs.
    /// </summary>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to _node which StopNode disposes.")]
    protected void StartNode()
    {
        if (_node is not null)
            return;

        BenchmarkRuntime.EnsureInitialized();
        _node = BenchmarkNodeScope.StartAsync(CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Stops the in-process benchmark node. Call from each benchmark class <c>[GlobalCleanup]</c>.
    /// </summary>
    protected void StopNode()
    {
        var node = _node;
        _node = null;
        node?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Connects a client and disposes it before returning.
    /// </summary>
    protected void ConnectAndDisposeClient()
    {
        StartNode();
        var lease = OpenClientLease();
        lease.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    /// <summary>
    /// Connects a client, resolves a cache handle, then disposes the client.
    /// </summary>
    /// <param name="cacheName">Cache name.</param>
    protected void GetCacheHandleAndDispose(string cacheName)
    {
        StartNode();
        var lease = OpenClientLease();
        try
        {
            _ = lease.Client.GetCacheAsync<object?>(cacheName, CancellationToken.None).AsTask().GetAwaiter().GetResult();
        }
        finally
        {
            lease.DisposeAsync().AsTask().GetAwaiter().GetResult();
        }
    }

    /// <summary>
    /// Opens a long-lived client and cache session on the benchmark node.
    /// </summary>
    /// <param name="cacheName">Cache name.</param>
    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to _cacheSession which StopSharedCache disposes.")]
    protected void StartSharedCache(string cacheName)
    {
        StartNode();
        _cacheSession = BenchmarkCacheSession.OpenAsync(RequireNode(), cacheName, CancellationToken.None).GetAwaiter().GetResult();
    }

    /// <summary>
    /// Disposes the shared cache session opened by <see cref="StartSharedCache" />.
    /// </summary>
    protected void StopSharedCache()
    {
        var session = _cacheSession;
        _cacheSession = null;
        session?.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Caller disposes the returned lease.")]
    private BenchmarkClientLease OpenClientLease() => RequireNode().OpenClientAsync(CancellationToken.None).GetAwaiter().GetResult();

    private BenchmarkNodeScope RequireNode() =>
        _node ?? throw new InvalidOperationException("Benchmark node was not started. Global setup did not run.");
}
