using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Internal.Threading;

/// <summary>Runs asynchronous work at most once and hands every caller the task of that single run.</summary>
internal static class AsyncLazyInitializer
{
    /// <summary>
    /// Starts <paramref name="action" /> on the first call for <paramref name="slot" />; every later call, concurrent or re-entrant,
    /// gets the same task, which completes when that run completes and carries its failure or cancellation.
    /// </summary>
    /// <remarks>A call made from inside the work must not await the returned task: the work would wait for itself.</remarks>
    /// <typeparam name="TState">The type of the state.</typeparam>
    /// <param name="slot">The owner's field that holds the task of the single run; <see langword="null" /> until the first call.</param>
    /// <param name="state">The state passed to <paramref name="action" />.</param>
    /// <param name="action">The work to run once, expected to be a <see langword="static" /> lambda so the call does not allocate a closure.</param>
    /// <returns>The task of the single run.</returns>
    internal static Task EnsureStartedAsync<TState>(ref Task? slot, TState state, Func<TState, Task> action)
    {
        ArgumentNullException.ThrowIfNull(action);
        var existing = Volatile.Read(ref slot);
        if (existing != null)
            return existing;

        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        existing = Interlocked.CompareExchange(ref slot, completion.Task, null);
        if (existing != null)
            return existing;

        // The task is published before the work starts, so a call made from inside the work gets it instead of starting a second run.
        _ = CompleteAsync(completion, state, action);
        return completion.Task;
    }

    private static async Task CompleteAsync<TState>(TaskCompletionSource completion, TState state, Func<TState, Task> action)
    {
        try
        {
            await action(state).ConfigureAwait(false);
            completion.SetResult();
        }
        catch (OperationCanceledException ex)
        {
            completion.SetCanceled(ex.CancellationToken);
        }
#pragma warning disable CA1031 // Every failure, including a synchronous throw from the action, must reach the callers through the shared task.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            completion.SetException(ex);
        }
    }
}
