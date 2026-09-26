using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Async counting semaphore that hands a released permit to the oldest queued waiter and releases every queued waiter on disposal.</summary>
/// <remarks>
/// The counting counterpart of <see cref="AsyncLock" /> (which is the single-permit case) with the same waiter contract. A free permit is taken
/// synchronously without allocating; a contended wait queues one waiter node (plus one cancellation registration for a cancelable token). A queued
/// waiter is released by exactly one event, whichever comes first: the hand-off of a permit, its own cancellation (the node leaves the queue at
/// once), or <see cref="Dispose" />, which faults it with <see cref="ObjectDisposedException" /> even when it waits on
/// <see cref="CancellationToken.None" />; a disposed <see cref="SemaphoreSlim" /> instead never completes a queued wait. Waiter continuations never
/// run inline on the completing thread.
/// <para>Permits held when the semaphore is disposed stay valid: releasing them never throws and only returns the permit.</para>
/// </remarks>
[ThreadSafe]
internal sealed class AsyncSemaphore : IDisposable
{
    private readonly Lock _sync = new();
    private int _available;
    private int _disposed;
    private Waiter? _head;
    private Waiter? _tail;

    /// <summary>Initializes a new instance of the <see cref="AsyncSemaphore" /> class.</summary>
    /// <param name="permits">The number of permits that can be held at the same time.</param>
    internal AsyncSemaphore(int permits)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(permits);
        _available = permits;
    }

    /// <summary>Refuses further waits and faults every queued waiter with <see cref="ObjectDisposedException" />; idempotent.</summary>
    /// <remarks>Permits already handed out are unaffected and can still be released.</remarks>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Waits read the flag under the lock, so none can queue once this lock section drains the queue.
        lock (_sync)
        {
            var waiter = _head;
            _head = null;
            _tail = null;
            while (waiter != null)
            {
                var next = waiter.Next;
                waiter.Detach();
                _ = waiter.TrySetException(new ObjectDisposedException(nameof(AsyncSemaphore)));
                waiter = next;
            }
        }
    }

    /// <summary>Returns a permit: hands it to the oldest queued waiter, or makes it available when none is queued.</summary>
    /// <remarks>Never throws; after <see cref="Dispose" /> the queue is empty, so it only makes the permit available.</remarks>
    internal void Release()
    {
        lock (_sync)
        {
            while (_head != null)
            {
                var waiter = _head;
                Unlink(waiter);
                if (waiter.TrySetResult())
                    return;
            }

            _available++;
        }
    }

    /// <summary>Takes a permit, waiting in FIFO order while none is available.</summary>
    /// <param name="cancellationToken">Cancels the wait while it is still queued.</param>
    /// <returns>A task that completes once the caller owns a permit and must <see cref="Release" /> it.</returns>
    /// <remarks>
    /// A disposed semaphore, or one disposed while the wait is queued, faults the result with <see cref="ObjectDisposedException" />; a canceled
    /// wait faults it with <see cref="OperationCanceledException" />.
    /// </remarks>
    internal ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return ValueTask.FromException(new ObjectDisposedException(nameof(AsyncSemaphore)));

            if (cancellationToken.IsCancellationRequested)
                return ValueTask.FromCanceled(cancellationToken);

            if (_available > 0)
            {
                _available--;
                return ValueTask.CompletedTask;
            }

            var waiter = new Waiter(this);
            Enqueue(waiter);

            // Registering under the lock keeps the registration visible to whoever dequeues the node; a token canceled
            // in the meantime runs the callback inline, which re-enters the lock and removes the node right away.
            if (cancellationToken.CanBeCanceled)
                waiter.Registration = cancellationToken.UnsafeRegister(CancelWaiter, waiter);

            return new ValueTask(waiter.Task);
        }
    }

    private static void CancelWaiter(object? state, CancellationToken cancellationToken)
    {
        if (state is Waiter waiter)
            waiter.Owner.Cancel(waiter, cancellationToken);
    }

    private void Cancel(Waiter waiter, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (!waiter.IsQueued)
                return;

            Unlink(waiter);
            _ = waiter.TrySetCanceled(cancellationToken);
        }
    }

    private void Enqueue(Waiter waiter)
    {
        waiter.IsQueued = true;
        waiter.Prev = _tail;
        if (_tail == null)
            _head = waiter;
        else
            _tail.Next = waiter;

        _tail = waiter;
    }

    private void Unlink(Waiter waiter)
    {
        if (waiter.Prev == null)
            _head = waiter.Next;
        else
            waiter.Prev.Next = waiter.Next;

        if (waiter.Next == null)
            _tail = waiter.Prev;
        else
            waiter.Next.Prev = waiter.Prev;

        waiter.Detach();
    }

    /// <summary>Queued wait: a completion source linked into the owner's FIFO queue, guarded by the owner's lock.</summary>
    private sealed class Waiter : TaskCompletionSource
    {
        internal Waiter(AsyncSemaphore owner)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            Owner = owner;
        }

        internal bool IsQueued { get; set; }

        internal Waiter? Next { get; set; }

        internal AsyncSemaphore Owner { get; }

        internal Waiter? Prev { get; set; }

        internal CancellationTokenRegistration Registration { private get; set; }

        /// <summary>Marks the node as out of the queue and drops its cancellation registration without waiting for a running callback.</summary>
        internal void Detach()
        {
            IsQueued = false;
            Next = null;
            Prev = null;
            _ = Registration.Unregister();
        }
    }
}
