using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Records bounded logical cache operation spans for the surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class TracingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly string _nodeId;

    internal TracingCacheDecorator(ILogicalNamespacedCache<T> inner, string nodeId)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? throw new ArgumentException("Node id is required.", nameof(nodeId)) : nodeId;
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.GetEntry,
        static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        CacheOperationClassifier.ClassifyNullableReferenceResult,
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.RemoveExpiration,
        static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
        new MutationKeyArgs(operationId, cacheName, key),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Set,
        () => _inner.SetEntryAsync(cacheName, key, entry, cancellationToken));

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Touch,
        static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
        new TouchArgs(operationId, cacheName, key, expiration),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddEntryAsync(cacheName, key, entry, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Get,
        () => _inner.GetValueAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheValueResult);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Remove,
        () => _inner.RemoveAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheRemoveResult);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Update,
        static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
        new UpdateArgs(operationId, cacheName, key, value),
        CacheOperationClassifier.ClassifyFoundBool,
        cancellationToken);

    private static string GetSpanName(string operation) => operation switch
    {
        CacheOperationNames.Get => SpanNames.Get,
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
        if (activity?.IsAllDataRequested is not true)
            return;

        _ = activity.SetTag("cache.result", result);
        if (!string.Equals(result, CacheOperationResults.Ok, StringComparison.OrdinalIgnoreCase))
            _ = activity.SetStatus(ActivityStatusCode.Error);
    }

    private Activity? StartActivity(string operation)
    {
        var activity = ActivitySourceHolder.StartInternal(GetSpanName(operation));
        if (activity?.IsAllDataRequested is not true)
            return activity;

        _ = activity.SetTag("cache.operation", operation);
        _ = activity.SetTag("squirix.node_id", _nodeId);
        return activity;
    }

    private async ValueTask TraceAsync<TState>(
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
        catch (TimeoutException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (OperationCanceledException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (ResourceExhaustedException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (JournalCapacityExceededException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (RpcException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (ArgumentException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        finally
        {
            RecordResult(activity, result);
        }
    }

    private async ValueTask<TResult> TraceAsync<TState, TResult>(
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
        catch (TimeoutException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (OperationCanceledException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (ResourceExhaustedException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (JournalCapacityExceededException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (RpcException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        catch (ArgumentException ex)
        {
            result = CacheOperationClassifier.ClassifyException(ex);
            throw;
        }
        finally
        {
            RecordResult(activity, result);
        }
    }

    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);

    private static class SpanNames
    {
        internal const string Get = "squirix.cache.get";
        internal const string GetEntry = "squirix.cache.get_entry";
        internal const string Remove = "squirix.cache.remove";
        internal const string RemoveExpiration = "squirix.cache.remove_expiration";
        internal const string Set = "squirix.cache.set";
        internal const string Touch = "squirix.cache.touch";
        internal const string TryAdd = "squirix.cache.try_add";
        internal const string Update = "squirix.cache.update";
    }
}
