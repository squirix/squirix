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

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetEntryAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.RemoveExpirationAsync(operationId, cacheName, key, ct), cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.SetEntryAsync(operationId, cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TouchAsync(operationId, cacheName, key, expiration, ct), cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.TryAddEntryAsync(operationId, cacheName, key, entry, ct), cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.GetValueAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.RemoveAsync(operationId, cacheName, key, ct), cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(ct => _inner.UpdateAsync(operationId, cacheName, key, value, ct), cancellationToken);

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
            return default;
        }
    }
}
