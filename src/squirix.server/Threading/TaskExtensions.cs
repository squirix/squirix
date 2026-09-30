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

    /// <summary>Disposes <paramref name="disposable" /> and returns any failure instead of throwing it, including one thrown before the disposal task is returned.</summary>
    /// <param name="disposable">The object to dispose.</param>
    /// <returns>The failure, or <see langword="null" /> when the disposal completed.</returns>
    internal static async ValueTask<Exception?> CaptureDisposeFailureAsync(this IAsyncDisposable disposable)
    {
        ArgumentNullException.ThrowIfNull(disposable);
        try
        {
            await disposable.DisposeAsync().ConfigureAwait(false);
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
