using System;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Benchmarks.Infrastructure;

/// <summary>
/// Owns one connected client and a single cache handle for cache-operation benchmarks.
/// </summary>
internal sealed class BenchmarkCacheSession : IAsyncDisposable
{
    private BenchmarkClientLease? _clientLease;
    private int _disposed;

    private BenchmarkCacheSession(BenchmarkClientLease clientLease, ICache<object?> cache)
    {
        _clientLease = clientLease;
        Cache = cache;
    }

    internal ICache<object?> Cache { get; }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        var clientLease = _clientLease;
        _clientLease = null;
        if (clientLease is not null)
            await clientLease.DisposeAsync().ConfigureAwait(false);
    }

    [SuppressMessage("Reliability", "CA2000:Dispose objects before losing scope", Justification = "Ownership transfers to BenchmarkCacheSession which disposes the client lease.")]
    internal static async Task<BenchmarkCacheSession> OpenAsync(BenchmarkNodeScope node, string cacheName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(node);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        var clientLease = await node.OpenClientAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var cache = await clientLease.Client.GetCacheAsync<object?>(cacheName, cancellationToken).ConfigureAwait(false);
            return new BenchmarkCacheSession(clientLease, cache);
        }
        catch (InvalidOperationException)
        {
            await clientLease.DisposeAsync().ConfigureAwait(false);
            throw;
        }
        catch (IOException)
        {
            await clientLease.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }
}
