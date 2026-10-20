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

    /// <summary>Invokes the action so a synchronous throw surfaces as a faulted task after the action has run its own cleanup.</summary>
    /// <typeparam name="TState">The action state type.</typeparam>
    /// <param name="state">The action state.</param>
    /// <param name="action">The work to run.</param>
    /// <param name="cancellationToken">The caller cancellation token passed to the action.</param>
    /// <returns>A task that mirrors the action task, or is faulted when the action throws synchronously.</returns>
#pragma warning disable ZA1101, MA0215 // The async state machine is what turns a synchronous throw into a faulted task.
    private static async Task<TResult> InvokeAsync<TState>(TState state, Func<TState, CancellationToken, Task<TResult>> action, CancellationToken cancellationToken) =>
        await action(state, cancellationToken).ConfigureAwait(false);
#pragma warning restore ZA1101, MA0215

    /// <summary>Runs the action on the calling thread until its first suspension, then completes the published flight; never throws.</summary>
    /// <typeparam name="TState">The action state type.</typeparam>
    /// <param name="key">The flight key.</param>
    /// <param name="state">The action state.</param>
    /// <param name="action">The work to run.</param>
    /// <param name="completion">The published flight, removed from the map before it is completed.</param>
    /// <param name="cancellationToken">The caller cancellation token passed to the action.</param>
    /// <returns>A task that completes once the flight has been completed.</returns>
    private async Task DriveAsync<TState>(
        string key,
        TState state,
        Func<TState, CancellationToken, Task<TResult>> action,
        TaskCompletionSource<TResult> completion,
        CancellationToken cancellationToken)
    {
        var work = InvokeAsync(state, action, cancellationToken);
        Task untyped = work;
        await untyped.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // Removal precedes completion so a continuation that calls RunAsync again starts a new flight.
        Remove(key, completion.Task);
        if (work.IsCanceled)
            _ = completion.TrySetCanceled(cancellationToken);
        else if (work.IsFaulted)
            _ = completion.TrySetException(work.Exception.InnerExceptions);
        else
            _ = completion.TrySetResult(await work.ConfigureAwait(false));
    }

    /// <summary>Removes the flight only while it is still the one published for the key, so a newer flight is never removed.</summary>
    /// <param name="key">The flight key.</param>
    /// <param name="flight">The finished flight.</param>
    private void Remove(string key, Task<TResult> flight) => _ = _concurrent.TryRemove(new KeyValuePair<string, Task<TResult>>(key, flight));
}
