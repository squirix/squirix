using System;
using System.Collections.Concurrent;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Internal;

/// <summary>Serializes asynchronous work per key so a factory runs at most once for concurrent callers.</summary>
/// <typeparam name="TResult">The result type produced by the single-flight factory.</typeparam>
[Immutable]
internal sealed class KeyedSingleFlight<TResult>
{
    private readonly ConcurrentDictionary<string, Task<TResult>> _concurrent = new(StringComparer.Ordinal);

    internal Task<TResult> RunAsync<TState>(string key, TState state, Func<TState, CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        return _concurrent.GetOrAdd(
            key,
            static (inFlightKey, runState) => runState.Flight.ExecuteAndCleanupAsync(inFlightKey, runState.State, runState.Action, runState.CancellationToken),
            new RunAsyncState<TState>(this, state, action, cancellationToken));
    }

    private async Task<TResult> ExecuteAndCleanupAsync<TState>(string key, TState state, Func<TState, CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        try
        {
            return await action(state, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            _ = _concurrent.TryRemove(key, out _);
        }
    }

    [Immutable]
    private readonly record struct RunAsyncState<TState>(
        KeyedSingleFlight<TResult> Flight,
        TState State,
        Func<TState, CancellationToken, Task<TResult>> Action,
        CancellationToken CancellationToken);
}
