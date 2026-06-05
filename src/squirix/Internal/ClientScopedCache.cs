using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal;

internal sealed class ClientScopedCache<T> : ICache<T>
{
    private readonly SquirixClient _client;
    private readonly ICache<T> _inner;

    public ClientScopedCache(SquirixClient client, ICache<T> inner)
    {
        _client = client ?? throw new ArgumentNullException(nameof(client));
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public Task AddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.AddAsync(key, value, options, cancellationToken);
    }

    public Task<CacheEntryResult<T>> GetEntryAsync(string key, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.GetEntryAsync(key, cancellationToken);
    }

    public Task<CacheExpirationResult> GetExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.GetExpirationAsync(key, cancellationToken);
    }

    public Task<CacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.GetValueAsync(key, cancellationToken);
    }

    public Task<CacheValueResult<T>> GetOrAddAsync(
        string key,
        Func<string, CancellationToken, Task<T?>> valueFactory,
        CacheEntryOptions? options = null,
        CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.GetOrAddAsync(key, valueFactory, options, cancellationToken);
    }

    public Task<bool> RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.RemoveAsync(key, cancellationToken);
    }

    public Task<bool> RemoveExpirationAsync(string key, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.RemoveExpirationAsync(key, cancellationToken);
    }

    public Task SetAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.SetAsync(key, value, options, cancellationToken);
    }

    public Task<bool> TouchAsync(string key, TimeSpan expiration, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.TouchAsync(key, expiration, cancellationToken);
    }

    public Task<bool> TouchAsync(string key, DateTimeOffset absoluteExpiration, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.TouchAsync(key, absoluteExpiration, cancellationToken);
    }

    public Task<bool> UpdateAsync(string key, T? value, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.UpdateAsync(key, value, cancellationToken);
    }

    public Task<bool> TryAddAsync(string key, T? value, CacheEntryOptions? options = null, CancellationToken cancellationToken = default)
    {
        _client.ThrowIfDisposed();
        return _inner.TryAddAsync(key, value, options, cancellationToken);
    }
}
