using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Utils;

/// <summary>Serializes asynchronous work per key so a factory runs at most once for concurrent callers.</summary>
/// <typeparam name="TResult">The result type produced by the single-flight factory.</typeparam>
internal sealed class KeyedSingleFlight<TResult>
{
    private readonly ConcurrentDictionary<string, Task<TResult>> _inFlight = new(StringComparer.Ordinal);

    public ValueTask<TResult> RunAsync(string key, Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var task = _inFlight.GetOrAdd(
            key,
            static (inFlightKey, state) => state.Flight.ExecuteAndCleanupAsync(inFlightKey, state.Action, state.CancellationToken),
            new RunAsyncState(this, action, cancellationToken));

        return new ValueTask<TResult>(task);
    }

    private async Task<TResult> ExecuteAndCleanupAsync(string key, Func<CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken)
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

    private readonly record struct RunAsyncState(KeyedSingleFlight<TResult> Flight, Func<CancellationToken, ValueTask<TResult>> Action, CancellationToken CancellationToken);
}
