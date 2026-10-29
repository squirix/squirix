using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Records bounded logical cache operation spans for the surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class TracingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly string _nodeId;

    internal TracingCacheDecorator(ILogicalNamespacedCache<T> inner, string nodeId)
    {
        ArgumentNullException.ThrowIfNull(inner);
        _inner = inner;
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? throw new ArgumentException("Node id is required.", nameof(nodeId)) : nodeId;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.GetEntry,
        static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        CacheOperationClassifier.ClassifyNullableReferenceResult,
        cancellationToken);

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Get,
        static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        CacheOperationClassifier.ClassifyCacheValueResult,
        cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Remove,
        static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
        new MutationKeyArgs(operationId, cacheName, key),
        CacheOperationClassifier.ClassifyCacheRemoveResult,
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.RemoveExpiration,
        static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
        new MutationKeyArgs(operationId, cacheName, key),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Set,
        static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Touch,
        static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
        new TouchArgs(operationId, cacheName, key, expiration),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryAdd,
        static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
        new SetEntryArgs<T>(operationId, cacheName, key, entry),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, CancellationToken cancellationToken) =>
        TraceAsync(
            CacheOperationNames.GetOrAdd,
            static (inner, args, ct) => inner.AddOrGetAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs<T>(operationId, cacheName, key, entry),
            CacheOperationClassifier.ClassifyAddOrGetResult,
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Update,
        static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
        new UpdateArgs<T>(operationId, cacheName, key, value),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    private static string GetSpanName(string operation) => operation switch
    {
        CacheOperationNames.Get => SpanNames.Get,
        CacheOperationNames.GetOrAdd => SpanNames.GetOrAdd,
        CacheOperationNames.GetEntry => SpanNames.GetEntry,
        CacheOperationNames.Remove => SpanNames.Remove,
        CacheOperationNames.RemoveExpiration => SpanNames.RemoveExpiration,
        CacheOperationNames.Set => SpanNames.Set,
        CacheOperationNames.Touch => SpanNames.Touch,
        CacheOperationNames.TryAdd => SpanNames.TryAdd,
        CacheOperationNames.Update => SpanNames.Update,
        _ => throw new ArgumentOutOfRangeException(nameof(operation), operation, "Unsupported cache operation."),
    };

    private static void RecordResult(Activity? activity, string result)
    {
        if (activity?.IsAllDataRequested != true)
            return;

        _ = activity.SetTag("cache.result", result);
        if (!string.Equals(result, CacheOperationResults.Ok, StringComparison.OrdinalIgnoreCase))
            _ = activity.SetStatus(ActivityStatusCode.Error);
    }

    private Activity? StartActivity(string operation)
    {
        var activity = ActivitySourceHolder.StartInternal(GetSpanName(operation));
        if (activity?.IsAllDataRequested != true)
            return activity;

        _ = activity.SetTag("cache.operation", operation);
        _ = activity.SetTag("squirix.node_id", _nodeId);
        return activity;
    }

    private ValueTask TraceAsync<TState>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken) =>
        ActivitySourceHolder.HasListeners
            ? TraceCoreAsync(operation, invoke, state, cancellationToken)
            : invoke(_inner, state, cancellationToken);

    /// <summary>Runs the operation inside a span that starts and ends in this async method.</summary>
    /// <typeparam name="TState">The operation argument type.</typeparam>
    /// <param name="operation">The cache operation name.</param>
    /// <param name="invoke">Calls the inner cache.</param>
    /// <param name="state">The operation arguments.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>A task that completes when the inner operation does.</returns>
    /// <remarks>
    /// Without a listener the decorator calls the inner cache directly and skips a state machine. With one, the span must not start in
    /// the synchronous caller: its activity would stay the caller's current one after the await.
    /// </remarks>
    private async ValueTask TraceCoreAsync<TState>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        using var activity = StartActivity(operation);
        var result = CacheOperationResults.Ok;
        try
        {
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
        catch (Exception ex) when (CacheOperationClassifier.IsClassifiedException(ex))
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        finally
        {
            RecordResult(activity, result);
        }
    }

    private ValueTask<TResult> TraceAsync<TState, TResult>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        Func<TResult, string> classifyResult,
        CancellationToken cancellationToken) =>
        ActivitySourceHolder.HasListeners
            ? TraceCoreAsync(operation, invoke, state, classifyResult, cancellationToken)
            : invoke(_inner, state, cancellationToken);

    /// <summary>Runs the operation inside a span that starts and ends in this async method, and classifies its result.</summary>
    /// <typeparam name="TState">The operation argument type.</typeparam>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="operation">The cache operation name.</param>
    /// <param name="invoke">Calls the inner cache.</param>
    /// <param name="state">The operation arguments.</param>
    /// <param name="classifyResult">Maps the result to the span result tag.</param>
    /// <param name="cancellationToken">Cancels the operation.</param>
    /// <returns>A task that yields the inner result.</returns>
    private async ValueTask<TResult> TraceCoreAsync<TState, TResult>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        Func<TResult, string> classifyResult,
        CancellationToken cancellationToken)
    {
        using var activity = StartActivity(operation);
        var result = CacheOperationResults.Ok;
        try
        {
            var value = await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
            result = classifyResult(value);
            return value;
        }
        catch (Exception ex) when (CacheOperationClassifier.IsClassifiedException(ex))
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        finally
        {
            RecordResult(activity, result);
        }
    }

    private static class SpanNames
    {
        internal const string Get = "squirix.cache.get";
        internal const string GetOrAdd = "squirix.cache.get_or_add";
        internal const string GetEntry = "squirix.cache.get_entry";
        internal const string Remove = "squirix.cache.remove";
        internal const string RemoveExpiration = "squirix.cache.remove_expiration";
        internal const string Set = "squirix.cache.set";
        internal const string Touch = "squirix.cache.touch";
        internal const string TryAdd = "squirix.cache.try_add";
        internal const string Update = "squirix.cache.update";
    }
}
