using System;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies runtime cache-operation backpressure before logical cache operations enter the inner runtime pipeline.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class BackpressureCacheDecorator<T> : IRequestCache<T>
{
    private readonly IBackpressureClientIdResolver _clientIdResolver;
    private readonly IBackpressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;

    internal BackpressureCacheDecorator(ILogicalNamespacedCache<T> inner, IBackpressureGate gate, IBackpressureClientIdResolver clientIdResolver)
    {
        ArgumentNullException.ThrowIfNull(inner);
        ArgumentNullException.ThrowIfNull(gate);
        ArgumentNullException.ThrowIfNull(clientIdResolver);
        _inner = inner;
        _gate = gate;
        _clientIdResolver = clientIdResolver;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.GetEntry,
        static (inner, args, context, ct) => inner.GetEntryForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.GetEntry,
        static (inner, args, context, ct) => inner.GetValueWithDeadlineForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Get,
        static (inner, args, context, ct) => inner.GetValueForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        request,
        cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Remove,
        static (inner, args, context, ct) => inner.RemoveForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        request,
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.RemoveExpiration,
        static (inner, args, context, ct) => inner.RemoveExpirationForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        request,
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Set,
        static (inner, args, context, ct) => inner.SetEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        request,
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Touch,
        static (inner, args, context, ct) => inner.TouchForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct, context),
        new TouchArgs(operationId, cacheName, key, expiration),
        request,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.TryAdd,
            static (inner, args, context, ct) => inner.TryAddEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            request,
            cancellationToken);

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Update,
            static (inner, args, context, ct) => inner.TryReplaceEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            request,
            cancellationToken);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.GetOrAdd,
            static (inner, args, context, ct) => inner.AddOrGetForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            request,
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Update,
        static (inner, args, context, ct) => inner.UpdateForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct, context),
        new UpdateArgs<T>(operationId, cacheName, key, value),
        request,
        cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask WithBackpressureAsync<TState>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await InboundAdmission.AcquireIfNeededAsync(_gate, CacheOperationNames.CacheTransport, operation, _clientIdResolver, request, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
            await invoke(_inner, state, request, cancellationToken).ConfigureAwait(false);
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<TResult> WithBackpressureAsync<TState, TResult>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await InboundAdmission.AcquireIfNeededAsync(_gate, CacheOperationNames.CacheTransport, operation, _clientIdResolver, request, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
            return await invoke(_inner, state, request, cancellationToken).ConfigureAwait(false);
    }
}
