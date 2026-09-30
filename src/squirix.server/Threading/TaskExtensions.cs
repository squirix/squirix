using System;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Helpers that observe the outcome of an awaitable without letting an expected failure escape.</summary>
/// <remarks>
/// Classic extension methods rather than an extension block: the analyzer that guards single consumption of a
/// <see cref="ValueTask" /> accepts it as the <see langword="this" /> argument but not as an extension-block receiver.
/// </remarks>
internal static class TaskExtensions
{
    /// <summary>Awaits the task and returns a failure <paramref name="filter" /> accepts instead of throwing it; any other failure propagates.</summary>
    /// <param name="task">The task to await.</param>
    /// <param name="filter">Selects the failures to capture, expected to be a <see langword="static" /> lambda so the call does not allocate a closure.</param>
    /// <returns>The captured failure, or <see langword="null" /> when the task completed successfully.</returns>
    internal static async ValueTask<Exception?> CaptureFailureAsync(this Task task, Func<Exception, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        try
        {
            await task.ConfigureAwait(false);
            return null;
        }
        catch (Exception ex) when (filter(ex))
        {
            return ex;
        }
    }

    /// <summary>Awaits the task and returns any failure instead of throwing it.</summary>
    /// <param name="task">The value task to await; this call consumes it.</param>
    /// <returns>The failure, or <see langword="null" /> when the task completed successfully.</returns>
    /// <remarks>For best-effort work such as shutdown drains, where one failure must not stop the rest and the caller reports it.</remarks>
    internal static async ValueTask<Exception?> CaptureFailureAsync(this ValueTask task)
    {
        try
        {
            await task.ConfigureAwait(false);
            return null;
        }
        catch (Exception ex)
        {
            return ex;
        }
    }

    /// <summary>Awaits the task and returns a failure <paramref name="filter" /> accepts instead of throwing it; any other failure propagates.</summary>
    /// <param name="task">The value task to await; this call consumes it.</param>
    /// <param name="filter">Selects the failures to capture, expected to be a <see langword="static" /> lambda so the call does not allocate a closure.</param>
    /// <returns>The captured failure, or <see langword="null" /> when the task completed successfully.</returns>
    internal static async ValueTask<Exception?> CaptureFailureAsync(this ValueTask task, Func<Exception, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        try
        {
            await task.ConfigureAwait(false);
            return null;
        }
        catch (Exception ex) when (filter(ex))
        {
            return ex;
        }
    }
}
