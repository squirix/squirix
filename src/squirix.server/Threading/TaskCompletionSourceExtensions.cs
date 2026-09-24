using System;
using System.Collections.Generic;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Helpers that complete <see cref="TaskCompletionSource" /> instances with the outcome of the work that fulfils them.</summary>
internal static class TaskCompletionSourceExtensions
{
    /// <param name="source">The completion source fulfilled by the operation.</param>
    extension(TaskCompletionSource source)
    {
        /// <summary>Runs <paramref name="action" /> and completes the source with its outcome; a failure faults the source and is rethrown.</summary>
        /// <typeparam name="TState">The type of the state handed to the operation.</typeparam>
        /// <param name="state">The state passed to <paramref name="action" />.</param>
        /// <param name="action">The operation, expected to be a <see langword="static" /> lambda or method group so the call does not allocate.</param>
        internal void Run<TState>(TState state, Action<TState> action)
        {
            try
            {
                action(state);
            }
            catch (Exception ex)
            {
                _ = source.TrySetException(ex);
                throw;
            }

            _ = source.TrySetResult();
        }

        /// <summary>Runs <paramref name="action" /> and completes the source with its outcome; a failure faults the source and is contained.</summary>
        /// <typeparam name="TState">The type of the state handed to the operation.</typeparam>
        /// <param name="state">The state passed to <paramref name="action" />.</param>
        /// <param name="action">The operation, expected to be a <see langword="static" /> lambda or method group so the call does not allocate.</param>
        internal void RunIsolated<TState>(TState state, Action<TState> action)
        {
            try
            {
                action(state);
            }
#pragma warning disable CA1031 // The failure is delivered to the awaiter of the source, so it must not escape to the caller
            catch (Exception ex)
#pragma warning restore CA1031
            {
                _ = source.TrySetException(ex);
                return;
            }

            _ = source.TrySetResult();
        }
    }

    /// <param name="sources">The completion sources to complete together.</param>
    extension(List<TaskCompletionSource> sources)
    {
        /// <summary>Completes every source in the list successfully; sources that are already completed are left untouched.</summary>
        internal void CompleteAll()
        {
            for (var i = 0; i < sources.Count; i++)
                _ = sources[i].TrySetResult();
        }

        /// <summary>Faults every source in the list with <paramref name="exception" />; sources that are already completed are left untouched.</summary>
        /// <param name="exception">The failure to deliver to the awaiters.</param>
        internal void FaultAll(Exception exception)
        {
            for (var i = 0; i < sources.Count; i++)
                _ = sources[i].TrySetException(exception);
        }

        /// <summary>Faults every source in the list that is still pending with <paramref name="exception" /> and counts them.</summary>
        /// <param name="exception">The failure to deliver to the awaiters.</param>
        /// <returns>The number of sources this call faulted; sources that are already completed are left untouched and not counted.</returns>
        internal int FaultPending(Exception exception)
        {
            var faulted = 0;
            for (var i = 0; i < sources.Count; i++)
            {
                if (sources[i].TrySetException(exception))
                    faulted++;
            }

            return faulted;
        }
    }
}
