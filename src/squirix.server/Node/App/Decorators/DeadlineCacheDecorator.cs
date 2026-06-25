using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Core;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies an optional default operation deadline to logical cache calls.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class DeadlineCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private const string PipelineDeadlineExceededMessage = "Logical cache operation exceeded the configured pipeline deadline.";

    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IOptions<CachePipelineDeadlineOptions> _options;

    internal DeadlineCacheDecorator(ILogicalNamespacedCache<T> inner, IOptions<CachePipelineDeadlineOptions> options)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.GetEntryAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.RemoveExpirationAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.SetEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.TouchAsync(args.OperationId, args.CacheName, args.Key, args.Expiration, ct),
            new TouchArgs(operationId, cacheName, key, expiration),
            cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.TryAddEntryAsync(args.OperationId, args.CacheName, args.Key, args.Entry, ct),
            new SetEntryArgs(operationId, cacheName, key, entry),
            cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.GetValueAsync(args.CacheName, args.Key, ct),
            new ReadKeyArgs(cacheName, key),
            cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.RemoveAsync(args.OperationId, args.CacheName, args.Key, ct),
            new MutationKeyArgs(operationId, cacheName, key),
            cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithDeadlineAsync(
            static (inner, args, ct) => inner.UpdateAsync(args.OperationId, args.CacheName, args.Key, args.Value, ct),
            new UpdateArgs(operationId, cacheName, key, value),
            cancellationToken);

    private async ValueTask WithDeadlineAsync<TState>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var configured = _options.Value.DefaultOperationTimeout;
        if (configured is null || configured.Value <= TimeSpan.Zero || cancellationToken.IsCancellationRequested)
        {
            await invoke(_inner, state, cancellationToken).ConfigureAwait(false);
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(budget);
        try
        {
            await invoke(_inner, state, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            if (ServerCancelClassifier.ClassifyLogicalPipelineDeadlineCancellation(cancellationToken, linked.Token) is ServerCancelScenarioKind.OperationDeadlineExceeded)
                throw new TimeoutException(PipelineDeadlineExceededMessage, ex);

            throw;
        }
    }

    private async ValueTask<TResult> WithDeadlineAsync<TState, TResult>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask<TResult>> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        var budget = _options.Value.DefaultOperationTimeout;
        if (budget is null || budget.Value <= TimeSpan.Zero)
            return await invoke(_inner, state, cancellationToken).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(budget);
        try
        {
            return await invoke(_inner, state, linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            if (ServerCancelClassifier.ClassifyLogicalPipelineDeadlineCancellation(cancellationToken, linked.Token) is ServerCancelScenarioKind.OperationDeadlineExceeded)
                throw new TimeoutException(PipelineDeadlineExceededMessage, ex);

            throw;
        }
    }

    private readonly record struct MutationKeyArgs(string OperationId, string CacheName, string Key);

    private readonly record struct ReadKeyArgs(string CacheName, string Key);

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, CacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);
}
