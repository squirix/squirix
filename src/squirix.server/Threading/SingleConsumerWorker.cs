using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Runs queued work items serially on one dedicated long-running worker thread.</summary>
/// <typeparam name="T">The type of queued work item.</typeparam>
internal sealed class SingleConsumerWorker<T> : IDisposable
{
    private const int DefaultStopBudgetSeconds = 30;

    private readonly Action<T> _handler;
    private readonly Action<T, Exception> _onFault;
    private readonly Action? _onStopTimeout;

    /// <summary>Guards the queue and the completed flag; it is a monitor rather than a <see cref="Lock" /> because the idle consumer waits on it.</summary>
    private readonly object _gate = new();
    private readonly Queue<QueuedItem> _queue = new();
    private readonly ManualResetEvent _stopped = new(false);
    private readonly TimeSpan _stopBudget;
    private readonly Thread _thread;
    private bool _completed;
    private int _abandoned;
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="SingleConsumerWorker{T}" /> class.</summary>
    /// <param name="handler">The handler invoked for each queued item.</param>
    /// <param name="onFault">
    /// Required callback invoked with the faulted item and its exception when a fire-and-forget item's handler throws, so the fault is surfaced rather than silently
    /// dropped. Completion-aware items surface faults only through their awaited <see cref="Task" />.
    /// </param>
    /// <param name="stopBudget">How long <see cref="Dispose" /> waits for the consumer thread, not negative; 30 seconds when <see langword="null" />.</param>
    /// <param name="onStopTimeout">Invoked when <see cref="Dispose" /> gives up waiting, so the leaked thread is reported.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="stopBudget" /> is negative.</exception>
    internal SingleConsumerWorker(Action<T> handler, Action<T, Exception> onFault, TimeSpan? stopBudget = null, Action? onStopTimeout = null)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(onFault);
        _handler = handler;
        _onFault = onFault;
        if (stopBudget < TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(nameof(stopBudget), stopBudget, "The stop budget must not be negative.");

        _stopBudget = stopBudget ?? TimeSpan.FromSeconds(DefaultStopBudgetSeconds);
        _onStopTimeout = onStopTimeout;
        _thread = new Thread(Run) { IsBackground = true, Name = $"SingleConsumerWorker<{typeof(T).Name}>" };
        _thread.Start();
    }

    /// <summary>Stops the worker within its stop budget (see <see cref="TryStop" />) and reports a timeout through the stop-timeout callback.</summary>
    public void Dispose()
    {
        if (TryStop(_stopBudget) || _onStopTimeout == null)
            return;

        // Disposal never throws; the callback is the reporting channel, and a failure of the channel
        // itself has nowhere to go without global state, so it is discarded by design.
        _ = Isolated.Run(_onStopTimeout, static callback => callback());
    }

    /// <summary>
    /// Marks the worker as completed and waits up to <paramref name="budget" /> for the consumer thread to drain the queued items and exit, then releases
    /// its resources.
    /// </summary>
    /// <param name="budget">How long to wait for the consumer thread.</param>
    /// <returns>
    /// <see langword="false" /> when the thread is still running a handler after <paramref name="budget" />: the items it has not yet taken are refused with
    /// <see cref="ObjectDisposedException" /> instead of run, and the queue and the thread are leaked because the handler may still use them. Otherwise
    /// <see langword="true" />, including a repeated call and a call from a handler, which cannot wait for its own thread and returns at once; the thread
    /// then exits once the queued items are drained.
    /// </returns>
    internal bool TryStop(TimeSpan budget)
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return true;

        // Completion is atomic with enqueueing: an item is either queued before it and drained, or refused after it.
        lock (_gate)
        {
            _completed = true;
            Monitor.Pulse(_gate);
        }

        if (Thread.CurrentThread == _thread)
            return true;

        if (!_stopped.WaitOne(budget))
        {
            Volatile.Write(ref _abandoned, 1);

            // The thread may have finished between the expiry and the flag; then nothing is leaked and there is nothing to report.
            if (!_stopped.WaitOne(0))
                return false;
        }

        _stopped.Dispose();
        return true;
    }

    /// <summary>Enqueues an item and returns a <see cref="Task" /> that completes when the handler has run (or faulted) on the dedicated worker thread.</summary>
    /// <param name="item">The item to process on the dedicated worker thread.</param>
    /// <returns>
    /// A <see cref="Task" /> that completes with the handler's result, or faults with the handler's exception or <see cref="ObjectDisposedException" /> if the worker is
    /// disposed.
    /// </returns>
    internal Task EnqueueAsync(T item)
    {
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (Volatile.Read(ref _disposed) == 1)
        {
            _ = completion.TrySetException(new ObjectDisposedException(typeof(SingleConsumerWorker<T>).FullName));
            return completion.Task;
        }

        if (!TryEnqueue(new QueuedItem(completion, item)))
            _ = completion.TrySetException(new ObjectDisposedException(typeof(SingleConsumerWorker<T>).FullName));

        return completion.Task;
    }

    /// <summary>Enqueues an item for fire-and-forget processing; returns immediately without waiting for the handler to run.</summary>
    /// <param name="item">The item to process on the dedicated worker thread.</param>
    /// <remarks>
    /// Does not throw synchronously. When the worker is disposed and the item cannot be enqueued, the failure is surfaced
    /// through <c language="csharp">onFault</c> rather than thrown to the caller.
    /// </remarks>
    internal void Post(T item)
    {
        var work = new QueuedItem(null, item);
        if (Volatile.Read(ref _disposed) == 1)
        {
            InvokeOnFault(item, new ObjectDisposedException(typeof(SingleConsumerWorker<T>).FullName));
            return;
        }

        if (!TryEnqueue(work))
            InvokeOnFault(item, new ObjectDisposedException(typeof(SingleConsumerWorker<T>).FullName));
    }

    private void InvokeOnFault(T item, Exception ex) =>
        // The onFault callback is the last reporting channel for a fire-and-forget fault. If it throws,
        // the fault has nowhere else to go without global state, so both failures are discarded by design
        // and the worker continues instead of crashing.
        _ = Isolated.Run((OnFault: _onFault, Item: item, Fault: ex), static state => state.OnFault(state.Item, state.Fault));

    private void Run()
    {
        try
        {
            // The consumer exits only when the queue is empty and completion is set. Items are queued only before completion, so every
            // accepted item has been handled by then.
            while (TryTake(out var work))
            {
                if (Volatile.Read(ref _abandoned) != 0)
                    Refuse(work);
                else if (work.Completion == null)
                    RunHandler(work.Item);
                else
                    RunCompletion(work);
            }
        }
        finally
        {
            _ = _stopped.Set();
        }
    }

    private bool TryEnqueue(QueuedItem work)
    {
        lock (_gate)
        {
            if (_completed)
                return false;

            _queue.Enqueue(work);
            Monitor.Pulse(_gate);
            return true;
        }
    }

    private bool TryTake(out QueuedItem work)
    {
        lock (_gate)
        {
            while (_queue.Count == 0 && !_completed)
                _ = Monitor.Wait(_gate);

            return _queue.TryDequeue(out work);
        }
    }

    private void Refuse(QueuedItem work)
    {
        var refusal = new ObjectDisposedException(typeof(SingleConsumerWorker<T>).FullName);
        if (work.Completion == null)
            InvokeOnFault(work.Item, refusal);
        else
            _ = work.Completion.TrySetException(refusal);
    }

    private void RunCompletion(QueuedItem work) => work.Completion?.RunIsolated(work.Item, _handler);

    private void RunHandler(T item)
    {
        // Worker isolation: one bad item must not kill the consumer thread.
        var failure = Isolated.Run((Handler: _handler, Item: item), static state => state.Handler(state.Item));
        if (failure != null)
            InvokeOnFault(item, failure);
    }

    [Immutable]
    private readonly record struct QueuedItem
    {
        public QueuedItem(TaskCompletionSource? completion, T item)
        {
            Completion = completion;
            Item = item;
        }

        public TaskCompletionSource? Completion { get; }

        public T Item { get; }
    }
}
