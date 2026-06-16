using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal;

/// <summary>Serializes asynchronous work per key so a factory runs at most once for concurrent callers.</summary>
/// <typeparam name="TResult">The result type produced by the single-flight factory.</typeparam>
internal sealed class KeyedSingleFlight<TResult>
{
    private readonly ConcurrentDictionary<string, Task<TResult>> _concurrent = new(StringComparer.Ordinal);

    public Task<TResult> RunAsync(string key, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var result = _concurrent.GetOrAdd(
            key,
            static (inFlightKey, state) => state.Flight.ExecuteAndCleanupAsync(inFlightKey, state.Action, state.CancellationToken),
            new RunAsyncState(this, action, cancellationToken));

        return result;
    }

    private async Task<TResult> ExecuteAndCleanupAsync(string key, Func<CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        try
        {
            return await action(cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _concurrent.TryRemove(key, out _);
        }
    }

    private readonly record struct RunAsyncState(KeyedSingleFlight<TResult> Flight, Func<CancellationToken, Task<TResult>> Action, CancellationToken CancellationToken);
}
