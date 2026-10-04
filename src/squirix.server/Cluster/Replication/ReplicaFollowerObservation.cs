using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Observes follower append tasks without letting abandoned work surface unobserved exceptions.</summary>
internal static class ReplicaFollowerObservation
{
    /// <summary>Waits for one follower append task and reports its outcome.</summary>
    /// <param name="replicaIndex">Zero-based replica slot of the follower.</param>
    /// <param name="followerTask">The follower append task.</param>
    /// <returns>The follower's acknowledgement, or none when its task faulted or was canceled.</returns>
    internal static async Task<FollowerCompletion> AwaitFollowerAsync(int replicaIndex, Task<ReplicaDurableAcknowledgement> followerTask)
    {
        // The raw follower task is observed without deadline cancellation: aborting the majority loop
        // is the outer WaitAsync's job, while late durable responses must still reach RecordAcknowledgement
        // through the background observe path instead of being converted into error completions.
        var singleton = new List<Task<ReplicaDurableAcknowledgement>>(1) { followerTask };
        _ = await Task.WhenAny(singleton).ConfigureAwait(false);
        if (followerTask.IsCompletedSuccessfully)
        {
            // ValueTask wraps the foreign follower task into one owned by this method (RemoteCache idiom).
            var acknowledgement = await new ValueTask<ReplicaDurableAcknowledgement>(followerTask).ConfigureAwait(false);
            return new FollowerCompletion(replicaIndex, acknowledgement);
        }

        // Faults and cancellations are intentionally indistinguishable here: both deweight the replica
        // to lagging without failing the majority. Touching Exception marks the fault observed, so a
        // faulted task never surfaces as unobserved.
        _ = followerTask.Exception;
        return new FollowerCompletion(replicaIndex, null);
    }

    /// <summary>Takes the next completed task, removing it from the pending list.</summary>
    /// <param name="pending">Remaining tasks to observe.</param>
    /// <param name="timeout">The longest wait for any task to complete.</param>
    /// <param name="timeProvider">The time source bounding the wait.</param>
    /// <param name="faultReporter">Receives the fault of an abandoned task that fails later, or <see langword="null" /> to only observe it.</param>
    /// <returns>The completed task.</returns>
    /// <exception cref="TimeoutException">
    /// The bound expired before any task completed; every remaining task got a fault-only exception
    /// observer, so abandoned follower work never surfaces unobserved exceptions.
    /// </exception>
    internal static async Task<Task> TakeNextCompletedAsync(List<Task> pending, TimeSpan timeout, TimeProvider timeProvider, Action<Exception>? faultReporter)
    {
        try
        {
            var completed = await Task.WhenAny(pending).WaitAsync(timeout, timeProvider, CancellationToken.None).ConfigureAwait(false);
            _ = pending.Remove(completed);
            return completed;
        }
        catch (TimeoutException)
        {
            ObserveAbandoned(pending, faultReporter);
            throw;
        }
    }

    /// <summary>Takes the next completed follower task, removing it from the pending list.</summary>
    /// <param name="pending">Remaining follower tasks to observe.</param>
    /// <param name="timeout">The longest wait for any task to complete.</param>
    /// <param name="timeProvider">The time source bounding the wait.</param>
    /// <param name="faultReporter">Receives the fault of an abandoned task that fails later, or <see langword="null" /> to only observe it.</param>
    /// <returns>The completed follower task.</returns>
    /// <exception cref="TimeoutException">
    /// The bound expired before any task completed; every remaining task got a fault-only exception
    /// observer, so abandoned follower work never surfaces unobserved exceptions.
    /// </exception>
    internal static async Task<Task<FollowerCompletion>> TakeNextCompletedAsync(
        List<Task<FollowerCompletion>> pending,
        TimeSpan timeout,
        TimeProvider timeProvider,
        Action<Exception>? faultReporter)
    {
        try
        {
            var completed = await Task.WhenAny(pending).WaitAsync(timeout, timeProvider, CancellationToken.None).ConfigureAwait(false);
            _ = pending.Remove(completed);
            return completed;
        }
        catch (TimeoutException)
        {
            ObserveAbandoned(pending, faultReporter);
            throw;
        }
    }

    private static void ObserveAbandoned(IReadOnlyList<Task> remaining, Action<Exception>? faultReporter)
    {
        foreach (var task in remaining)
        {
            _ = task.ContinueWith(
                static (t, state) =>
                {
                    // Only a faulted task runs this continuation, so the exception is never null.
                    if (t.Exception is { } fault)
                        (state as Action<Exception>)?.Invoke(fault);
                },
                faultReporter,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }
    }
}
