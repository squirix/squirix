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

/// <summary>Records generic logical cache operation metrics for the surface.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class MetricsCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;

    public MetricsCacheDecorator(ILogicalNamespacedCache<T> inner)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.GetEntry,
            static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            CacheOperationClassifier.ClassifyNullableReferenceResult,
            cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.RemoveExpiration,
            static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            CacheOperationClassifier.ClassifyFoundBool,
            cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.Set,
            static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.Touch,
            static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
            new TouchArgs(operationId, cacheName, key, expiration),
            CacheOperationClassifier.ClassifyFoundBool,
            cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.TryAdd,
            static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            CacheOperationClassifier.ClassifyFoundBool,
            cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.Get,
            static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            CacheOperationClassifier.ClassifyCacheValueResult,
            cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.Remove,
            static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            CacheOperationClassifier.ClassifyCacheRemoveResult,
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        ObserveAsync(
            cacheName,
            CacheOperationNames.Update,
            static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
            new UpdateArgs(operationId, cacheName, key, value),
            CacheOperationClassifier.ClassifyFoundBool,
            cancellationToken);

    private static void Record(string cacheName, string operation, string result, long startTimestamp) => CacheMetrics.RecordOperation(
        CacheName.NormalizeUnvalidated(cacheName),
        operation,
        result,
        Stopwatch.GetElapsedTime(startTimestamp).TotalSeconds);

    private async ValueTask ObserveAsync<TState>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
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

    private async ValueTask<TResult> ObserveAsync<TState, TResult>(
        string cacheName,
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        Func<TResult, string> classifyResult,
        CancellationToken cancellationToken)
    {
        var startTimestamp = Stopwatch.GetTimestamp();
        try
        {
            var value = await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
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

    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, CacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);
}
