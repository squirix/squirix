using System;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Maps transport-level <see cref="RpcException" /> failures from clustered remote calls where a stable normalization exists.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class DomainErrorMappingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    internal DomainErrorMappingCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
            new TouchArgs(operationId, cacheName, key, expiration),
            cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithMappingAsync(
            static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
            new UpdateArgs(operationId, cacheName, key, value),
            cancellationToken);

    private async ValueTask WithMappingAsync<TState>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        try
        {
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
        }
    }

    private async ValueTask<TResult> WithMappingAsync<TState, TResult>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        try
        {
            return await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
        catch (RpcException ex)
        {
            DomainTransportErrorMapper.Map(ex, cancellationToken);
            return default;
        }
    }

    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, CacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);
}
