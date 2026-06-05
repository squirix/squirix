using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Utils;

/// <summary>
/// Serializes asynchronous work per key so a factory runs at most once for concurrent callers.
/// </summary>
internal sealed class KeyedSingleFlight
{
    private readonly ConcurrentDictionary<string, Task> _inFlight = new(StringComparer.Ordinal);

    public ValueTask<TResult> RunAsync<TResult>(string key, Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var task = (Task<TResult>)_inFlight.GetOrAdd(key, _ => ExecuteAndCleanupAsync(key, action, cancellationToken));

        return new ValueTask<TResult>(task);
    }

    private async Task<TResult> ExecuteAndCleanupAsync<TResult>(string key, Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        try
        {
            return await action(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _inFlight.TryRemove(key, out _);
        }
    }
}
