using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks pending durability waits for <see cref="JournalCoordinator" />.</summary>
internal sealed class DurabilityAckRegistry
{
    private static readonly List<TaskCompletionSource> EmptyWaits = [];

    private readonly Lock _sync = new();
    private List<TaskCompletionSource> _waits = [];
    private List<TaskCompletionSource> _waitsSpare = [];
    private Exception? _failure;

    /// <summary>
    /// Creates a completion source for one durability wait; continuations never run inline on the journal thread.
    /// A fault-only observer swallows exceptions of orphaned sources — a waiter that left through
    /// <c>WaitAsync</c> cancellation while the journal thread later failed its source would otherwise
    /// surface as an unobserved task exception at GC time.
    /// </summary>
    /// <returns>A new completion source for a single durability wait.</returns>
    internal static TaskCompletionSource NewWait()
    {
        var wait = TaskCompletionSourceFactory.Create();
        _ = wait.Task.ContinueWith(static faulted => _ = faulted.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
        return wait;
    }

    /// <summary>
    /// Registers a pending wait. Throws <see cref="InvalidOperationException" /> once the pipeline has
    /// failed: the wait would otherwise park forever behind a dead journal thread, so it is rejected
    /// with the failure reason instead. The latch check and registration are atomic against
    /// <see cref="FailAll" />, closing the register-after-failure sweep window.
    /// </summary>
    /// <param name="wait">The completion source to register.</param>
    /// <exception cref="InvalidOperationException">Thrown when the pipeline has already failed.</exception>
    internal void Add(TaskCompletionSource wait)
    {
        lock (_sync)
        {
            if (_failure is { } failure)
                throw new InvalidOperationException("journal I/O thread failed.", failure);

            _waits.Add(wait);
        }
    }

    internal bool Remove(TaskCompletionSource wait)
    {
        lock (_sync)
            return _waits.Remove(wait);
    }

    /// <summary>Latches the failure reason and takes every pending wait; later <see cref="Add" /> calls are rejected.</summary>
    /// <param name="reason">The pipeline failure reason to latch.</param>
    /// <returns>The pending waits captured at failure time.</returns>
    internal List<TaskCompletionSource> FailAll(Exception reason)
    {
        lock (_sync)
        {
            _failure ??= reason;
            if (_waits.Count == 0)
                return EmptyWaits;

            return SwapOutWaits();
        }
    }

    private List<TaskCompletionSource> SwapOutWaits()
    {
        var batch = _waits;
        _waits = _waitsSpare;
        _waits.Clear();
        _waitsSpare = batch;
        return batch;
    }
}
