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
        WithDeadlineAsync(ct => _inner.GetEntryAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.RemoveExpirationAsync(operationId, cacheName, key, ct), cancellationToken);

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.SetEntryAsync(operationId, cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.TouchAsync(operationId, cacheName, key, expiration, ct), cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.TryAddEntryAsync(operationId, cacheName, key, entry, ct), cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.GetValueAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string operationId, string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.RemoveAsync(operationId, cacheName, key, ct), cancellationToken);

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.UpdateAsync(operationId, cacheName, key, value, ct), cancellationToken);

    private async ValueTask WithDeadlineAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var configured = _options.Value.DefaultOperationTimeout;
        if (configured is null || configured.Value <= TimeSpan.Zero || cancellationToken.IsCancellationRequested)
        {
            budget = TimeSpan.Zero;
            return false;
        }

        budget = configured.Value;
        return true;
    }

    private async ValueTask WithDeadlineAsync<TState>(
        Func<ILogicalNamespacedCache<T>, TState, CancellationToken, ValueTask> invoke,
        TState state,
        CancellationToken cancellationToken)
    {
        if (!ShouldApplyPipelineDeadline(cancellationToken, out var budget))
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
        if (!ShouldApplyPipelineDeadline(cancellationToken, out var budget))
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

    private readonly record struct SetEntryArgs(string OperationId, string CacheName, string Key, NodeCacheEntry<T> Entry);

    private readonly record struct TouchArgs(string OperationId, string CacheName, string Key, TimeSpan Expiration);

    private readonly record struct UpdateArgs(string OperationId, string CacheName, string Key, T? Value);
}
