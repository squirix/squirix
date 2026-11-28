using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;

namespace Squirix.Server.Runtime.Contracts;

/// <summary>Entry-based cache API for a single namespace; gRPC handlers translate wire requests into these operations.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal interface ICacheApi<T>
{
    ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<NodeCacheValueResult<T>> GetValueAsync(string key, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<NodeCacheValueResult<T>> GetValueUtf8Async(Utf8CacheKey key, CancellationToken cancellationToken, ICacheRequestContext? context = null) =>
        GetValueAsync(Encoding.UTF8.GetString(key.Bytes.Span), cancellationToken, context);

    ValueTask<bool> RemoveAsync(string operationId, string key, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<bool> RemoveExpirationAsync(string operationId, string key, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask SetEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<bool> TouchAsync(string operationId, string key, TimeSpan expiration, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<bool> TryAddEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    /// <summary>Adds with its operation identifier, or returns the existing value when the add did not take effect.</summary>
    /// <param name="operationId">The mutation operation identifier.</param>
    /// <param name="key">The key.</param>
    /// <param name="entry">The entry to add.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="context">The inbound request context when present.</param>
    /// <returns>The add outcome and existing value.</returns>
    ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<bool> TryReplaceEntryAsync(string operationId, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken, ICacheRequestContext? context = null);

    ValueTask<bool> UpdateAsync(string operationId, string key, T? value, CancellationToken cancellationToken, ICacheRequestContext? context = null);
}
