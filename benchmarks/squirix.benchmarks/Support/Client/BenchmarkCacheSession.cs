using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Benchmarks.Support.Client;

/// <summary>Owns one connected client and a single cache handle for cache-operation benchmarks.</summary>
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
        if (clientLease != null)
            await clientLease.DisposeAsync().ConfigureAwait(false);
    }

    internal static async Task<BenchmarkCacheSession> OpenAsync(Uri uri, string cacheName, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(uri);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);

        BenchmarkClientLease? clientLease = null;
        try
        {
            clientLease = await BenchmarkClientLease.ConnectAsync(uri, cancellationToken).ConfigureAwait(false);
            var cache = await clientLease.Client.GetCacheAsync<object?>(cacheName, cancellationToken).ConfigureAwait(false);
            var session = new BenchmarkCacheSession(clientLease, cache);
            clientLease = null;
            return session;
        }
        finally
        {
            if (clientLease != null)
                await clientLease.DisposeAsync().ConfigureAwait(false);
        }
    }
}
