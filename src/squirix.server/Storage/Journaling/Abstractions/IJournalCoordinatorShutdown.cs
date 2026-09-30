using System;
using System.Threading.Tasks;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>
/// Stops a journal coordinator explicitly, before disposal, so a failure to drain and make its frames durable reaches the caller instead of
/// being lost in a disposal that must not throw.
/// </summary>
internal interface IJournalCoordinatorShutdown
{
    /// <summary>
    /// Refuses new work, drains and makes durable every frame already accepted, and releases the journal thread and its resources, within
    /// the configured shutdown budget.
    /// </summary>
    /// <returns>A task that completes when the journal is stopped.</returns>
    /// <remarks>
    /// An attempt that leaves the journal thread alive fails with a <see cref="TimeoutException" /> and is not terminal: new work stays refused,
    /// callers waiting on a durable write are released with a commit-unknown failure, and the accepted frames stay queued, so a live journal
    /// thread still writes them and a later call resumes the stop from where it stopped (the marker is never enqueued twice). The stop is terminal
    /// once the journal thread exited and the resources were released; later calls then report the same outcome as the call that finished it,
    /// which is a success only when no accepted frame was lost.
    /// </remarks>
    /// <exception cref="TimeoutException">A stop stage did not finish in time; the journal thread and its resources stay open (not terminal).</exception>
    /// <exception cref="System.IO.IOException">
    /// The final write or flush failed, or accepted frames were abandoned without being written (terminal); acknowledged frames may not be durable.
    /// </exception>
    /// <exception cref="InvalidOperationException">The journal thread latched this failure (terminal); acknowledged frames may not be durable.</exception>
    /// <exception cref="ObjectDisposedException">The journal thread latched this failure for a reason other than a refused shutdown step (terminal).</exception>
    ValueTask StopAsync();
}
