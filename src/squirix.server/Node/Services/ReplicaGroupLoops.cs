using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Node.Services;

/// <summary>Waits for the per-group loops of a replication service, so one group's fault stops the others and then the service.</summary>
internal static class ReplicaGroupLoops
{
    /// <summary>Waits until every loop has ended; the first loop that fails cancels the others, and its fault is rethrown once they ended.</summary>
    /// <param name="loops">The running loops, each observing <paramref name="stopping" />; emptied by the wait.</param>
    /// <param name="stopping">The token source the loops observe, linked to the host stopping token.</param>
    /// <returns>A task that completes when every loop has ended, faulted with the first failure.</returns>
    /// <remarks>
    /// A loop that ends canceled is a normal shutdown only when the host or a failed loop asked for it; otherwise it counts as a failure.
    /// No loop still runs when the returned task completes, so nothing of a group outlives the service.
    /// </remarks>
    internal static async Task AwaitAllAsync(List<Task> loops, CancellationTokenSource stopping)
    {
        Task? failed = null;
        while (loops.Count > 0)
        {
            var ended = await Task.WhenAny(loops).ConfigureAwait(false);
            _ = loops.Remove(ended);
            if (failed != null || (!ended.IsFaulted && (!ended.IsCanceled || stopping.IsCancellationRequested)))
                continue;

            failed = ended;
            await stopping.CancelAsync().ConfigureAwait(false);
        }

        if (failed != null)
            await failed.ConfigureAwait(false);
    }
}
