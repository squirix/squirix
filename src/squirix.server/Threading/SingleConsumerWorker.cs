using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Runs queued work items serially on one dedicated long-running worker thread.</summary>
/// <typeparam name="T">The type of queued work item.</typeparam>
internal sealed class SingleConsumerWorker<T> : IDisposable
{
    private readonly Action<T> _handler;
    private readonly Action<T, Exception> _onFault;

    private readonly BlockingCollection<QueuedItem> _queue = new(new ConcurrentQueue<QueuedItem>());
    private readonly ManualResetEvent _stopped = new(false);
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="SingleConsumerWorker{T}" /> class.</summary>
    /// <param name="handler">The handler invoked for each queued item.</param>
    /// <param name="onFault">
    /// Required callback invoked with the faulted item and its exception when a fire-and-forget item's handler throws, so the fault is surfaced rather than silently
    /// dropped. Completion-aware items surface faults only through their awaited <see cref="Task" />.
    /// </param>
    internal SingleConsumerWorker(Action<T> handler, Action<T, Exception> onFault)
    {
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(onFault);
        _handler = handler;
        _onFault = onFault;
        var thread = new Thread(Run) { IsBackground = true, Name = $"SingleConsumerWorker<{typeof(T).Name}>" };
        thread.Start();
    }

    /// <summary>Marks the worker as completed, drains queued items, and waits for the consumer thread to fully exit before releasing its resources.</summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _queue.CompleteAdding();
        _ = _stopped.WaitOne();
        _queue.Dispose();
        _stopped.Dispose();
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
            _ = completion.TrySetException(new ObjectDisposedException(GetType().FullName));
            return completion.Task;
        }

        try
        {
            _queue.Add(new QueuedItem(completion, item), CancellationToken.None);
        }
        catch (Exception exception) when (exception is InvalidOperationException or ObjectDisposedException)
        {
            _ = completion.TrySetException(exception as ObjectDisposedException ?? new ObjectDisposedException(GetType().FullName));
        }

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
            InvokeOnFault(item, new ObjectDisposedException(GetType().FullName));
            return;
        }

        try
        {
            _queue.Add(work, CancellationToken.None);
        }
        catch (Exception ex) when (ex is InvalidOperationException or ObjectDisposedException)
        {
            InvokeOnFault(item, ex);
        }
    }

    private void InvokeOnFault(T item, Exception ex)
    {
        try
        {
            _onFault(item, ex);
        }
#pragma warning disable CA1031 // Worker isolation: one bad item must not kill the consumer thread
        catch (Exception exception)
#pragma warning restore CA1031
        {
            // The onFault callback is the last reporting channel for a fire-and-forget fault. If it throws,
            // the fault has nowhere else to go, so surface both the original handler exception and the onFault
            // failure via an error trace to keep the loss observable in production. A debug assertion is
            // deliberately avoided here: this degraded path must be logged and the worker continue, not crash.
            Trace.TraceError($"SingleConsumerWorker: onFault threw while reporting {typeof(T).Name} fault (original: {ex.GetType().Name}). {exception}");
        }
    }

    private void Run()
    {
        try
        {
            foreach (var work in _queue.GetConsumingEnumerable(CancellationToken.None))
            {
                if (work.Completion == null)
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

    private void RunCompletion(QueuedItem work)
    {
        var completion = work.Completion;
        if (completion == null)
            return;

        try
        {
            _handler(work.Item);
            _ = completion.TrySetResult();
        }
#pragma warning disable CA1031 // Worker isolation: one bad item must not kill the consumer thread
        catch (Exception exception)
#pragma warning restore CA1031
        {
            _ = completion.TrySetException(exception);
        }
    }

    private void RunHandler(T item)
    {
        try
        {
            _handler(item);
        }
#pragma warning disable CA1031 // Worker isolation: one bad item must not kill the consumer thread
        catch (Exception exception)
#pragma warning restore CA1031
        {
            InvokeOnFault(item, exception);
        }
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
