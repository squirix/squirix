using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Options;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Applies an optional default operation deadline to logical cache calls.</summary>
/// <typeparam name="T">The cache value type.</typeparam>
internal sealed class DeadlineCacheDecorator<T> : ILogicalNamespacedCache<T>
{
    private const string PipelineDeadlineExceededMessage = "Logical cache operation exceeded the configured pipeline deadline.";

    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly IOptions<CachePipelineDeadlineOptions> _options;

    public DeadlineCacheDecorator(ILogicalNamespacedCache<T> inner, IOptions<CachePipelineDeadlineOptions> options)
    {
        _inner = inner ?? throw new ArgumentNullException(nameof(inner));
        _options = options ?? throw new ArgumentNullException(nameof(options));
    }

    public ValueTask<CacheEntry<T>?> GetEntryAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.GetEntryAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> RemoveExpirationAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.RemoveExpirationAsync(cacheName, key, ct), cancellationToken);

    public ValueTask SetEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.SetEntryAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<bool> TouchAsync(string cacheName, string key, TimeSpan expiration, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.TouchAsync(cacheName, key, expiration, ct), cancellationToken);

    public ValueTask<bool> TryAddEntryAsync(string cacheName, string key, CacheEntry<T> entry, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.TryAddEntryAsync(cacheName, key, entry, ct), cancellationToken);

    public ValueTask<CacheValueResult<T>> GetValueAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.GetValueAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<CacheRemoveResult<T>> RemoveAsync(string cacheName, string key, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.RemoveAsync(cacheName, key, ct), cancellationToken);

    public ValueTask<bool> UpdateAsync(string cacheName, string key, T? value, CancellationToken cancellationToken) =>
        WithDeadlineAsync(ct => _inner.UpdateAsync(cacheName, key, value, ct), cancellationToken);

    private async ValueTask WithDeadlineAsync(Func<CancellationToken, ValueTask> action, CancellationToken cancellationToken)
    {
        var budget = _options.Value.DefaultOperationTimeout;
        if (budget is null || budget.Value <= TimeSpan.Zero)
        {
            await action(cancellationToken).ConfigureAwait(false);
            return;
        }

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(budget.Value);
        try
        {
            await action(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            if (OperationCancellationClassifier.ClassifyLogicalPipelineDeadlineCancellation(cancellationToken, linked.Token) is CancellationScenarioKind.OperationDeadlineExceeded)
            {
                throw new TimeoutException(PipelineDeadlineExceededMessage, ex);
            }

            throw;
        }
    }

    private async ValueTask<TResult> WithDeadlineAsync<TResult>(Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        var budget = _options.Value.DefaultOperationTimeout;
        if (budget is null || budget.Value <= TimeSpan.Zero)
            return await action(cancellationToken).ConfigureAwait(false);

        using var linked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        linked.CancelAfter(budget.Value);
        try
        {
            return await action(linked.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            if (OperationCancellationClassifier.ClassifyLogicalPipelineDeadlineCancellation(cancellationToken, linked.Token) is CancellationScenarioKind.OperationDeadlineExceeded)
            {
                throw new TimeoutException(PipelineDeadlineExceededMessage, ex);
            }

            throw;
        }
    }
}
