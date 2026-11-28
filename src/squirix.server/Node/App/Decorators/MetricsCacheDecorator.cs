using System;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Records generic logical cache operation metrics for the surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class MetricsCacheDecorator<T> : IRequestCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    private readonly CacheMetrics _metrics;

    internal MetricsCacheDecorator(ILogicalNamespacedCache<T> inner, CacheMetrics metrics)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _metrics = metrics;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.GetEntry,
        static (inner, args, context, ct) => inner.GetEntryForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        CacheOperationClassifier.ClassifyNullableReferenceResult,
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.GetEntry,
        static (inner, args, context, ct) => inner.GetValueWithDeadlineForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        static result => result.Found ? CacheOperationResults.Ok : CacheOperationResults.NotFound,
        request,
        cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Get,
        static (inner, args, context, ct) => inner.GetValueForRequestAsync(args.CacheName, args.Key, ct, context),
        new ReadKeyArgs(cacheName, key),
        CacheOperationClassifier.ClassifyCacheValueResult,
        request,
        cancellationToken);

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Remove,
        static (inner, args, context, ct) => inner.RemoveForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.RemoveExpiration,
        static (inner, args, context, ct) => inner.RemoveExpirationForRequestAsync(args.OperationId, args.CacheName, args.Key, ct, context),
        new MutationKeyArgs(operationId, cacheName, key),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Set,
        static (inner, args, context, ct) => inner.SetEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        request,
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Touch,
        static (inner, args, context, ct) => inner.TouchForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct, context),
        new TouchArgs(operationId, cacheName, key, expiration),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.TryAdd,
        static (inner, args, context, ct) => inner.TryAddEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Update,
        static (inner, args, context, ct) => inner.TryReplaceEntryForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.GetOrAdd,
            static (inner, args, context, ct) => inner.AddOrGetForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct, context),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            CacheOperationClassifier.ClassifyAddOrGetResult,
            request,
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Update,
        static (inner, args, context, ct) => inner.UpdateForRequestAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct, context),
        new UpdateArgs<T>(operationId, cacheName, key, value),
        CacheOperationClassifier.ClassifyFoundBool,
        request,
        cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask ObserveAsync<TState>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken) =>
        _metrics.Enabled
            ? ObserveCoreAsync(cacheName, operation, invoke, state, request, cancellationToken)
            : invoke(_inner, state, request, cancellationToken);

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private ValueTask<TResult> ObserveAsync<TState, TResult>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        Func<TResult, string> classifyResult,
        ICacheRequestContext? request,
        CancellationToken cancellationToken) =>
        _metrics.Enabled
            ? ObserveCoreAsync(cacheName, operation, invoke, state, classifyResult, request, cancellationToken)
            : invoke(_inner, state, request, cancellationToken);

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder))]
    private async ValueTask ObserveCoreAsync<TState>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask> invoke,
        TState state,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        var result = CacheOperationResults.Ok;
        try
        {
            await invoke(_inner, state, request, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (CacheOperationClassifier.IsClassifiedException(ex))
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        finally
        {
            Record(cacheName, operation, result, startTimestamp);
        }
    }

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    private async ValueTask<TResult> ObserveCoreAsync<TState, TResult>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, ICacheRequestContext?, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        Func<TResult, string> classifyResult,
        ICacheRequestContext? request,
        CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var value = await invoke(_inner, state, request, cancellationToken).ConfigureAwait(false);
            Record(cacheName, operation, classifyResult(value), startTimestamp);
            return value;
        }
        catch (Exception ex) when (CacheOperationClassifier.IsClassifiedException(ex))
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
    }

    private void Record(string cacheName, string operation, string result, long startTimestamp) => _metrics.RecordOperation(
        ServerCacheName.NormalizeUnvalidated(cacheName),
        operation,
        result,
        Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds);
}
