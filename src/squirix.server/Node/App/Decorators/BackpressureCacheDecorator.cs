using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies runtime cache-operation backpressure before logical cache operations enter the inner runtime pipeline.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class BackpressureCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private const string Transport = "cache";

    private readonly IBackpressureClientIdResolver _clientIdResolver;
    private readonly IBackpressureGate _gate;
    private readonly ILogicalNamespacedCache<T> _inner;

    internal BackpressureCacheDecorator(ILogicalNamespacedCache<T> inner, IBackpressureGate gate, IBackpressureClientIdResolver clientIdResolver)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _gate = gate ?? throw new ArgumentNullException(nameof(gate));
        _clientIdResolver = clientIdResolver ?? throw new ArgumentNullException(nameof(clientIdResolver));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.GetEntry,
        static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
        new ReadKeyArgs(cacheName, key),
        cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.RemoveExpiration,
        () => _inner.RemoveExpirationAsync(operationId, cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Set,
        () => _inner.SetEntryAsync(operationId, cacheName, key, entry, cancellationToken),
        cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Touch,
        () => _inner.TouchAsync(operationId, cacheName, key, expiration, cancellationToken),
        cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.TryAdd,
        () => _inner.TryAddEntryAsync(operationId, cacheName, key, entry, cancellationToken),
        cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureReadAsync(cacheName, key, cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Remove,
        () => _inner.RemoveAsync(operationId, cacheName, key, cancellationToken),
        cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) => WithBackpressureAsync(
        CacheOperationNames.Update,
        () => _inner.UpdateAsync(operationId, cacheName, key, value, cancellationToken),
        cancellationToken);

    private async ValueTask WithBackpressureAsync<TState>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var task = action();
        using (lease)
        {
            await task.ConfigureAwait(false);
        }
    }

    private static async ValueTask<TResult> RunWithLeaseAsync<TResult>(Func<ValueTask<TResult>> action, BackpressureLease lease)
    {
        using (lease)
        {
            return await action().ConfigureAwait(false);
        }
    }

    private async ValueTask<CacheValueResult<T>> RunWithLeaseForGetAsync(string cacheName, string key, BackpressureLease lease, CancellationToken cancellationToken)
    {
        using (lease)
        {
            return await _inner.GetValueAsync(cacheName, key, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask WithBackpressureAsync(string operation, Func<ValueTask> action, CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(Transport, operation, ClientId, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<TResult> WithBackpressureAsync<TState, TResult>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(Transport, operation, _clientIdResolver.Resolve(), cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw ServerOpContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
            return await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<CacheValueResult<T>> WithBackpressureReadAsync(string cacheName, string key, CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(Transport, CacheOperationNames.Get, ClientId, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        return await RunWithLeaseForGetAsync(cacheName, key, lease, cancellationToken).ConfigureAwait(false);
    }
}
