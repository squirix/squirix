using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Operations;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Records generic logical cache operation metrics for the surface.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class MetricsCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public MetricsCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, value, cancellationToken));

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, entry, cancellationToken));

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Contains,
        () => _inner.ContainsAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.GetEntry,
        () => _inner.GetEntryAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyNullableReferenceResult);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.GetExpiration,
        () => _inner.GetExpirationAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyNullableValueResult);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Get,
        () => _inner.GetValueAsync(cacheName, key, cancellationToken),
        static _ => CacheOperationResults.Ok);

    public ValueTask SetAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Set,
        () => _inner.SetAsync(cacheName, key, value, cancellationToken));

    public ValueTask SetAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Set,
        () => _inner.SetAsync(cacheName, key, entry, cancellationToken));

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.RemoveExpiration,
        () => _inner.RemoveExpirationAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Remove,
        () => _inner.RemoveAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.Touch,
        () => _inner.TouchAsync(cacheName, key, expiration, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, value, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, entry, cancellationToken),
        CacheOperationClassifier.ClassifyFoundBool);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.TryGet,
        () => _inner.TryGetValueAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheValueResult);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => ObserveAsync(
        cacheName,
        CacheOperationNames.TryRemove,
        () => _inner.TryRemoveAsync(cacheName, key, cancellationToken),
        CacheOperationClassifier.ClassifyCacheRemoveResult);

    private static async ValueTask ObserveAsync(string cacheName, string operation, Func<ValueTask> action)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
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
            Record(cacheName, operation, result, startTimestamp);
        }
    }

    private static async ValueTask<TResult> ObserveAsync<TResult>(string cacheName, string operation, Func<ValueTask<TResult>> action, Func<TResult, string> classifyResult)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var value = await action().ConfigureAwait(false);
            Record(cacheName, operation, classifyResult(value), startTimestamp);
            return value;
        }
        catch (TimeoutException ex)
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
        catch (OperationCanceledException ex)
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
        catch (ResourceExhaustedException ex)
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
        catch (RpcException ex)
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
        catch (ArgumentException ex)
        {
            Record(cacheName, operation, CacheOperationClassifier.ClassifyException(ex), startTimestamp);
            throw;
        }
    }

    private static void Record(string cacheName, string operation, string result, long startTimestamp) => CacheMetrics.RecordOperation(
        CacheName.NormalizeUnvalidated(cacheName),
        operation,
        result,
        Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds);
}
