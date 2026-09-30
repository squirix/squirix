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
    /// An attempt that leaves the journal thread alive fails with a <see cref="TimeoutException" /> and is not terminal: a later call can
    /// finish the stop. After the journal stopped, later calls report the same outcome as the call that stopped it.
    /// </remarks>
    /// <exception cref="TimeoutException">A stop stage did not finish in time; the journal thread and its resources stay open.</exception>
    /// <exception cref="System.IO.IOException">The final write or flush failed, so acknowledged frames may not be durable.</exception>
    ValueTask StopAsync();
}
