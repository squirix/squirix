using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Internal;

/// <summary>
/// Serializes asynchronous work per key so a factory runs at most once for concurrent callers. A flight is published before its work starts
/// and removed before its task completes, so a call made after a flight finished always starts a new one.
/// </summary>
/// <typeparam name="TResult">The result type produced by the single-flight factory.</typeparam>
[Immutable]
internal sealed class KeyedSingleFlight<TResult>
{
    private readonly ConcurrentDictionary<string, Task<TResult>> _concurrent = new(StringComparer.Ordinal);

    internal Task<TResult> RunAsync<TState>(string key, TState state, Func<TState, CancellationToken, Task<TResult>> action, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        if (_concurrent.TryGetValue(key, out var running))
            return running;

        var completion = new TaskCompletionSource<TResult>(TaskCreationOptions.RunContinuationsAsynchronously);
        var published = _concurrent.GetOrAdd(key, completion.Task);
        if (!ReferenceEquals(published, completion.Task))
            return published;

        _ = DriveAsync(key, state, action, completion, cancellationToken);
        return completion.Task;
    }

    /// <summary>Runs the action on the calling thread until its first suspension, then completes the published flight; never throws.</summary>
    /// <typeparam name="TState">The action state type.</typeparam>
    /// <param name="key">The flight key.</param>
    /// <param name="state">The action state.</param>
    /// <param name="action">The work to run.</param>
    /// <param name="completion">The published flight, completed after it is removed from the map.</param>
    /// <param name="cancellationToken">The caller cancellation token passed to the action.</param>
    /// <returns>A task that completes once the flight has been completed.</returns>
    private async Task DriveAsync<TState>(
        string key,
        TState state,
        Func<TState, CancellationToken, Task<TResult>> action,
        TaskCompletionSource<TResult> completion,
        CancellationToken cancellationToken)
    {
        TResult result;
        try
        {
            result = await action(state, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException ex)
        {
            Remove(key, completion.Task);
            _ = completion.TrySetCanceled(ex.CancellationToken);
            return;
        }
        catch (Exception ex) when (Fault(key, completion, ex))
        {
            return;
        }

        Remove(key, completion.Task);
        _ = completion.TrySetResult(result);
    }

    /// <summary>Removes the flight, then faults it with the action failure so the exception reaches every caller sharing the flight.</summary>
    /// <param name="key">The flight key.</param>
    /// <param name="completion">The published flight.</param>
    /// <param name="failure">The action failure.</param>
    /// <returns>Always <see langword="true" /> so the failure is handled by the flight and not rethrown from the driver.</returns>
    private bool Fault(string key, TaskCompletionSource<TResult> completion, Exception failure)
    {
        Remove(key, completion.Task);
        _ = completion.TrySetException(failure);
        return true;
    }

    /// <summary>Removes the flight only while it is still the one published for the key, so a newer flight is never removed.</summary>
    /// <param name="key">The flight key.</param>
    /// <param name="flight">The finished flight.</param>
    private void Remove(string key, Task<TResult> flight) => _ = _concurrent.TryRemove(new KeyValuePair<string, Task<TResult>>(key, flight));
}
