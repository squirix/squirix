using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Maps transport-level <see cref="RpcException" /> failures from clustered remote calls where a stable normalization exists.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class DomainErrorMappingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public DomainErrorMappingCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.AddAsync(cacheName, key, value, ct), cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.AddAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.ContainsAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetEntryAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetExpirationAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetValueAsync(cacheName, key, ct), cancellationToken);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.SetAsync(cacheName, key, value, ct), cancellationToken);

    public ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.SetAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.RemoveExpirationAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.RemoveAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TouchAsync(cacheName, key, expiration, ct), cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TryAddAsync(cacheName, key, value, ct), cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TryAddAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetOrAddAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.UpdateAsync(cacheName, key, value, ct), cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TryGetValueAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TryRemoveAsync(cacheName, key, ct), cancellationToken);

    private static async ValueTask WithMappingAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        try
        {
            await action(cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
        }
    }

    private static async ValueTask<TResult> WithMappingAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        try
        {
            return await action(cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
            return default!;
        }
    }
}
