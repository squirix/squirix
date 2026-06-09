using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Operations;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>
/// Applies runtime cache-operation backpressure before logical cache operations enter the inner runtime pipeline.
/// </summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class BackpressureCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private const string ClientId = "runtime";
    private const string Transport = "cache";

    private readonly IBackpressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;

    public BackpressureCacheDecorator(ILogicalNamespacedCache<T> inner, IBackpressureGate gate)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _gate = gate ?? throw new ArgumentNullException(nameof(gate));
    }

    public ValueTask AddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, value, cancellationToken),
        cancellationToken);

    public ValueTask AddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Add,
        () => _inner.AddAsync(cacheName, key, entry, cancellationToken),
        cancellationToken);

    public ValueTask<bool> ContainsAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Contains,
        () => _inner.ContainsAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.GetEntry,
        () => _inner.GetEntryAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<TimeSpan?> GetExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.GetExpiration,
        () => _inner.GetExpirationAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<T?> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Get,
        () => _inner.GetValueAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Insert,
        () => _inner.InsertAsync(cacheName, key, value, cancellationToken),
        cancellationToken);

    public ValueTask InsertAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Insert,
        () => _inner.InsertAsync(cacheName, key, entry, cancellationToken),
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.RemoveExpiration,
        () => _inner.RemoveExpirationAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<bool> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Remove,
        () => _inner.RemoveAsync(cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Touch,
        () => _inner.TouchAsync(cacheName, key, expiration, cancellationToken),
        cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, value, cancellationToken),
        cancellationToken);

    public ValueTask<bool> TryAddAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddAsync(cacheName, key, entry, cancellationToken),
        cancellationToken);

    public ValueTask<CacheValueResult<T>> TryGetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureReadAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> TryRemoveAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.TryRemove,
        () => _inner.TryRemoveAsync(cacheName, key, cancellationToken),
        cancellationToken);

    private static ValueTask RunWithLease(Func<ValueTask> action, BackpressureLease lease)
    {
        var task = action();
        if (!task.IsCompleted)
            return RunWithLeaseAwaited(lease, task);

        using (lease)
        {
            task.GetAwaiter().GetResult();
        }

        return ValueTask.CompletedTask;
    }

    private static ValueTask<TResult> RunWithLease<TResult>(Func<ValueTask<TResult>> action, BackpressureLease lease)
    {
        var task = action();
        if (!task.IsCompletedSuccessfully)
            return RunWithLeaseAwaited(lease, task);

        using (lease)
        {
            return ValueTask.FromResult(task.Result);
        }
    }

    private static async ValueTask RunWithLeaseAwaited(BackpressureLease lease, ValueTask task)
    {
        using (lease)
        {
            await task.ConfigureAwait(false);
        }
    }

    private static async ValueTask<TResult> RunWithLeaseAwaited<TResult>(BackpressureLease lease, ValueTask<TResult> task)
    {
        using (lease)
        {
            return await task.ConfigureAwait(false);
        }
    }

    private static async ValueTask WithBackpressureAwaited(ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> acquireTask, Func<ValueTask> action)
    {
        var (decision, lease) = await acquireTask.ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        await RunWithLease(action, lease).ConfigureAwait(false);
    }

    private static async ValueTask<TResult> WithBackpressureAwaited<TResult>(
        ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> acquireTask,
        Func<ValueTask<TResult>> action)
    {
        var (decision, lease) = await acquireTask.ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        return await RunWithLease(action, lease).ConfigureAwait(false);
    }

    private ValueTask<CacheValueResult<T>> RunWithLeaseForTryGet(string cacheName, string key, BackpressureLease lease, CancellationToken cancellationToken)
    {
        var task = _inner.TryGetValueAsync(cacheName, key, cancellationToken);
        if (!task.IsCompletedSuccessfully)
            return RunWithLeaseAwaited(lease, task);

        using (lease)
        {
            return ValueTask.FromResult(task.Result);
        }
    }

    private ValueTask WithBackpressureAsync(string operation, Func<ValueTask> action, CancellationToken cancellationToken)
    {
        var acquireTask = _gate.AcquireAsync(Transport, operation, ClientId, cancellationToken);
        if (!acquireTask.IsCompletedSuccessfully)
            return WithBackpressureAwaited(acquireTask, action);

        var (decision, lease) = acquireTask.Result;
        return !decision.IsAccepted ? throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown") : RunWithLease(action, lease);
    }

    private ValueTask<TResult> WithBackpressureAsync<TResult>(string operation, Func<ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        var acquireTask = _gate.AcquireAsync(Transport, operation, ClientId, cancellationToken);
        if (!acquireTask.IsCompletedSuccessfully)
            return WithBackpressureAwaited(acquireTask, action);

        var (decision, lease) = acquireTask.Result;
        return !decision.IsAccepted ? throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown") : RunWithLease(action, lease);
    }

    private ValueTask<CacheValueResult<T>> WithBackpressureReadAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var acquireTask = _gate.AcquireAsync(Transport, CacheOperationNames.TryGet, ClientId, cancellationToken);
        if (!acquireTask.IsCompletedSuccessfully)
            return WithBackpressureTryGetAwaited(acquireTask, cacheName, key, cancellationToken);

        var (decision, lease) = acquireTask.Result;
        return !decision.IsAccepted
            ? throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown")
            : RunWithLeaseForTryGet(cacheName, key, lease, cancellationToken);
    }

    private async ValueTask<CacheValueResult<T>> WithBackpressureTryGetAwaited(
        ValueTask<(BackpressureDecision Decision, BackpressureLease Lease)> acquireTask,
        string cacheName,
        string key,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await acquireTask.ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        return await RunWithLeaseForTryGet(cacheName, key, lease, cancellationToken).ConfigureAwait(false);
    }
}
