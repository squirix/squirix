using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Operations;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies runtime cache-operation backpressure before logical cache operations enter the inner runtime pipeline.</summary>
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

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.GetEntry,
            static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.RemoveExpiration,
            static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Set,
            static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Touch,
            static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
            new TouchArgs(operationId, cacheName, key, expiration),
            cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.TryAdd,
            static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Get,
            static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Remove,
            static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithBackpressureAsync(
            CacheOperationNames.Update,
            static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
            new UpdateArgs(operationId, cacheName, key, value),
            cancellationToken);

    private async ValueTask WithBackpressureAsync<TState>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(Transport, operation, ClientId, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
        {
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
    }

    private async ValueTask<TResult> WithBackpressureAsync<TState, TResult>(
        string operation,
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var (decision, lease) = await _gate.AcquireAsync(Transport, operation, ClientId, cancellationToken).ConfigureAwait(false);
        if (!decision.IsAccepted)
            throw CacheOperationContract.TooManyRequests(decision.RejectReason ?? "unknown");

        using (lease)
        {
            return await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
        }
    }

    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, CacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);
}
