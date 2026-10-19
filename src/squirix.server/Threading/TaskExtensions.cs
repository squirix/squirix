using System;
using System.Runtime.ExceptionServices;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Helpers that observe the outcome of an awaitable without letting an expected failure escape.</summary>
/// <remarks>
/// Classic extension methods rather than an extension block: the analyzer that guards single consumption of a
/// <see cref="ValueTask" /> accepts it as the <see langword="this" /> argument but not as an extension-block receiver.
/// A failed task is observed through its state rather than by awaiting it with a catch, so capturing a failure does not
/// raise a first-chance exception.
/// </remarks>
internal static class TaskExtensions
{
    /// <param name="task">The task to await.</param>
    extension(Task task)
    {
        /// <summary>Awaits the task and returns any failure instead of throwing it.</summary>
        /// <returns>The failure, or <see langword="null" /> when the task completed successfully.</returns>
        internal async ValueTask<Exception?> CaptureFailureAsync()
        {
            ArgumentNullException.ThrowIfNull(task);
            await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return FailureOf(task);
        }

        /// <summary>Awaits the task and returns a failure <paramref name="filter" /> accepts instead of throwing it; any other failure propagates.</summary>
        /// <param name="filter">Selects the failures to capture, expected to be a <see langword="static" /> lambda so the call does not allocate a closure.</param>
        /// <returns>The captured failure, or <see langword="null" /> when the task completed successfully.</returns>
        internal async ValueTask<Exception?> CaptureFailureAsync(Func<Exception, bool> filter)
        {
            ArgumentNullException.ThrowIfNull(filter);
            await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            return Filtered(FailureOf(task), filter);
        }
    }

    /// <summary>Disposes <paramref name="disposable" /> and returns any failure instead of throwing it, including one thrown before the disposal task is returned.</summary>
    /// <param name="disposable">The object to dispose.</param>
    /// <returns>The failure, or <see langword="null" /> when the disposal completed.</returns>
    internal static async ValueTask<Exception?> CaptureFailureAsync(this IAsyncDisposable disposable)
    {
        ArgumentNullException.ThrowIfNull(disposable);
        ValueTask disposal;
        try
        {
            disposal = disposable.DisposeAsync();
        }
        catch (Exception ex)
        {
            return ex;
        }

        if (disposal.IsCompletedSuccessfully)
        {
            await disposal.ConfigureAwait(false);
            return null;
        }

        var task = disposal.AsTask();
        await task.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        return FailureOf(task);
    }

    /// <summary>Awaits the task and returns a failure <paramref name="filter" /> accepts instead of throwing it; any other failure propagates.</summary>
    /// <param name="task">The value task to await; this call consumes it.</param>
    /// <param name="filter">Selects the failures to capture, expected to be a <see langword="static" /> lambda so the call does not allocate a closure.</param>
    /// <returns>The captured failure, or <see langword="null" /> when the task completed successfully.</returns>
    internal static async ValueTask<Exception?> CaptureFailureAsync(this ValueTask task, Func<Exception, bool> filter)
    {
        ArgumentNullException.ThrowIfNull(filter);
        if (task.IsCompletedSuccessfully)
        {
            await task.ConfigureAwait(false);
            return null;
        }

        var asTask = task.AsTask();
        await asTask.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        return Filtered(FailureOf(asTask), filter);
    }

    /// <summary>Returns the exception an await of <paramref name="completed" /> would have thrown, without throwing it.</summary>
    /// <param name="completed">The task whose awaiting has finished.</param>
    /// <returns>The failure, or <see langword="null" /> when the task completed successfully.</returns>
    private static Exception? FailureOf(Task completed)
    {
        if (completed.IsCanceled)
            return new TaskCanceledException(completed);

        return completed.IsFaulted ? completed.Exception.InnerException : null;
    }

    /// <summary>Returns <paramref name="failure" /> when <paramref name="filter" /> accepts it; otherwise rethrows it preserving its stack.</summary>
    /// <param name="failure">The captured failure, if any.</param>
    /// <param name="filter">Selects the failures to return.</param>
    /// <returns>The accepted failure, or <see langword="null" /> when there was none.</returns>
    private static Exception? Filtered(Exception? failure, Func<Exception, bool> filter)
    {
        if (failure == null || filter(failure))
            return failure;

        ExceptionDispatchInfo.Throw(failure);
        return failure;
    }
}
