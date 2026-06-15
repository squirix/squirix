using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Operations;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Records bounded logical cache operation spans for the surface.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class TracingCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly string _nodeId;

    public TracingCacheDecorator(ILogicalNamespacedCache<T> inner, string nodeId)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _nodeId = string.IsNullOrWhiteSpace(nodeId) ? throw new ArgumentException("Node id is required.", nameof(nodeId)) : nodeId;
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, value, cancellationToken));

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, entry, cancellationToken));

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Contains,
        () => _inner.ContainsAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.GetEntry,
        () => _inner.GetEntryAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyNullableReferenceResult);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.GetExpiration,
        () => _inner.GetExpirationAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyNullableValueResult);

    public ValueTask<CacheValueResult<T>> GetOrAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.GetOrAdd,
        () => _inner.GetOrAddAsync(cacheName, key, entry, cancellationToken),
        CacheOperationClassifier.ClassifyCacheValueResult);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Get,
        () => _inner.GetValueAsync(cacheName, key, cancellationToken),
        static _ => CacheOperationResults.Ok);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Remove,
        () => _inner.RemoveAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.RemoveExpiration,
        () => _inner.RemoveExpirationAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Set,
        () => _inner.SetAsync(cacheName, key, value, cancellationToken));

    public ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Set,
        () => _inner.SetAsync(cacheName, key, entry, cancellationToken));

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Touch,
        () => _inner.TouchAsync(cacheName, key, expiration, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, value, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, entry, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryGet,
        () => _inner.TryGetValueAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheValueResult);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.TryRemove,
        () => _inner.TryRemoveAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheRemoveResult);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => TraceAsync(
        CacheOperationNames.Update,
        () => _inner.UpdateAsync(cacheName, key, value, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    private static string GetSpanName(string operation) => $"squirix.cache.{operation}";

    private static void RecordResult(Activity? activity, string result)
    {
        _ = activity?.SetTag("cache.result", result);
        if (!string.Equals(result, CacheOperationResults.Ok, StringComparison.OrdinalIgnoreCase))
            _ = activity?.SetStatus(ActivityStatusCode.Error);
    }

    private Activity? StartActivity(string operation)
    {
        var activity = ActivitySourceHolder.StartInternal(GetSpanName(operation));
        _ = activity?.SetTag("cache.operation", operation);
        _ = activity?.SetTag("squirix.node_id", _nodeId);
        return activity;
    }

    private async ValueTask TraceAsync(string operation, Func<ValueTask> action)
    {
        using var activity = StartActivity(operation);
        var result = CacheOperationResults.Ok;
        try
        {
            await action().ConfigureAwait(false);
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

    private async ValueTask<TResult> TraceAsync<TResult>(string operation, Func<ValueTask<TResult>> action, Func<TResult, string> classifyResult)
    {
        using var activity = StartActivity(operation);
        var result = CacheOperationResults.Ok;
        try
        {
            var value = await action().ConfigureAwait(false);
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
}
