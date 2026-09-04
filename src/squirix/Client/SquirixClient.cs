using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;

namespace Squirix.Client;

/// <summary>Entry point to connect to Squirix servers and get typed cache instances.</summary>
public sealed class SquirixClient : ISquirixClient
{
    private readonly IRemoteClientSession _remoteSession;
    private int _disposeOnce;
    private int _disposed;

    private SquirixClient(IRemoteClientSession remoteSession)
    {
        ArgumentNullException.ThrowIfNull(remoteSession);
        _remoteSession = remoteSession;
    }

    /// <summary>Connects to a Squirix server endpoint.</summary>
    /// <param name="endpoint">The Squirix server endpoint URI.</param>
    /// <param name="cancellationToken">Cancellation token for client warm-up.</param>
    /// <returns>A remote <see cref="ISquirixClient" /> session.</returns>
    public static ValueTask<ISquirixClient> ConnectAsync(Uri endpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);

        var options = new SquirixClientOptions();
        options.Endpoints.Add(endpoint);

        return ConnectAsync(options, null, cancellationToken);
    }

    /// <summary>Connects to Squirix server bootstrap endpoints using client-only options.</summary>
    /// <remarks>
    /// At least one configured endpoint must be reachable; additional endpoints provide transport failover.
    /// See <see cref="SquirixClientOptions.Endpoints" /> for HA semantics.
    /// </remarks>
    /// <param name="configure">Configures remote client endpoints and transport settings.</param>
    /// <param name="cancellationToken">Cancellation token for client warm-up.</param>
    /// <returns>A remote <see cref="ISquirixClient" /> session.</returns>
    public static ValueTask<ISquirixClient> ConnectAsync(Action<SquirixClientOptions> configure, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SquirixClientOptions();
        configure(options);

        return ConnectAsync(options, null, cancellationToken);
    }

    /// <summary>
    /// Ends the logical client session. Idempotent. InternalCache facades obtained via <see cref="GetCacheAsync{T}" /> throw
    /// <see cref="ObjectDisposedException" /> on subsequent operations. Remote transport resources owned by this session are released.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> that completes when disposal finishes.</returns>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.CompareExchange(ref _disposeOnce, 1, 0) != 0)
            return;

        _ = Interlocked.Exchange(ref _disposed, 1);
        await _remoteSession.DisposeAsync().ConfigureAwait(false);
    }

    /// <summary>
    /// Returns the primary <see cref="ICache{T}" /> facade for a logical cache name.
    /// </summary>
    /// <typeparam name="T">The value type stored in the cache.</typeparam>
    /// <param name="cacheName">The logical cache name to access.</param>
    /// <param name="cancellationToken">A cancellation token used during cache resolution.</param>
    /// <returns>A non-owning <see cref="ICache{T}" /> facade for the specified cache name.</returns>
    public ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfDisposed();

        var cache = _remoteSession.GetCache<T>(cacheName);
        return ValueTask.FromResult<ICache<T>>(new InternalCache<T>(ThrowIfDisposed, cache));
    }

    internal static async ValueTask<ISquirixClient> ConnectAsync(SquirixClientOptions options, HttpMessageHandler? handler, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(options);

        var session = await RemoteClientSessionFactory.ConnectAsync(options.Endpoints, options.BearerTokenProvider, options.Serializer, handler, cancellationToken)
                                                      .ConfigureAwait(false);
        return new SquirixClient(session);
    }

    private void ThrowIfDisposed() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);

    [Immutable]
    private sealed class InternalCache<T> : ICache<T>
    {
        private readonly ICache<T> _inner;
        private readonly Action _throwIfDisposed;

        internal InternalCache(Action throwIfDisposed, ICache<T> inner)
        {
            ArgumentNullException.ThrowIfNull(throwIfDisposed);
            ArgumentNullException.ThrowIfNull(inner);
            _throwIfDisposed = throwIfDisposed;
            _inner = inner;
        }

        public Task AddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.AddAsync(key, value, options, cancellationToken);
        }

        public Task<CacheEntryResult<T>> GetEntryAsync(string key, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.GetEntryAsync(key, cancellationToken);
        }

        public Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.GetExpirationAsync(key, cancellationToken);
        }

        public Task<CacheValueResult<T>> GetOrAddAsync(
            string key,
            Func<string, CancellationToken, Task<T?>> valueFactory,
            CacheEntryOptions? options = null,
            CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.GetOrAddAsync(key, valueFactory, options, cancellationToken);
        }

        public Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.GetValueAsync(key, cancellationToken);
        }

        public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.RemoveAsync(key, cancellationToken);
        }

        public Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.RemoveExpirationAsync(key, cancellationToken);
        }

        public Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.SetAsync(key, value, options, cancellationToken);
        }

        public Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.TouchAsync(key, expiration, cancellationToken);
        }

        public Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.TouchAsync(key, absoluteExpiration, cancellationToken);
        }

        public Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.TryAddAsync(key, value, options, cancellationToken);
        }

        public Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default)
        {
            _throwIfDisposed();
            return _inner.UpdateAsync(key, value, cancellationToken);
        }
    }
}
