using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>Async mutual-exclusion lock that hands ownership to queued waiters in FIFO order.</summary>
/// <remarks>
/// Pair with <see cref="AsyncLockHolder"/> through <see langword="using"/> to release the lock on the scope exit.
/// <para>
/// A free lock is taken synchronously without allocating. A contended acquisition queues one waiter node (plus one
/// cancellation registration for a cancelable token); a release hands ownership to the oldest queued waiter, so
/// acquisition order is arrival order (the journal order follows the <c language="csharp">MutationGate</c> order). Every queued waiter
/// is released by exactly one event, whichever comes first: the hand-off, its own cancellation (the node leaves the
/// queue at once), or <see cref="Dispose"/>, which faults it with <see cref="ObjectDisposedException"/> even when it
/// waits on <see cref="CancellationToken.None"/>. Waiter continuations never run inline on the completing thread.
/// </para>
/// <para>
/// Every acquisition, whether a free take or a hand-off, issues a new ownership generation that the returned
/// <see cref="AsyncLockHolder"/> exposes as its <see cref="AsyncLockOwnership"/>: surfaces the lock protects check that
/// ownership instead of asking whether the lock is held at all, so they tell the holder from any other flow.
/// </para>
/// <para>
/// Disposal does not revoke the current holder: it keeps exclusion until it releases, and that release only marks
/// the lock free. Shape after
/// <see href="https://devblogs.microsoft.com/dotnet/building-async-coordination-primitives-part-6-asynclock/">Building Async Coordination Primitives, Part 6: AsyncLock (Stephen Toub)</see>
/// and <see href="https://github.com/StephenCleary/AsyncEx/blob/master/src/Nito.AsyncEx.Coordination/AsyncLock.cs">Nito.AsyncEx.AsyncLock</see>.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class AsyncLock : IDisposable
{
    private readonly Lock _sync = new();
    private int _disposed;
    private Waiter? _head;

    /// <summary>The generation of the current holder, or zero while the lock is free; written only under <see cref="_sync"/>.</summary>
    private ulong _holderGeneration;

    /// <summary>The last generation issued; guarded by <see cref="_sync"/>. Starts at zero, so no acquisition ever gets generation zero.</summary>
    private ulong _lastGeneration;

    private Waiter? _tail;

    /// <summary>Refuses further acquisitions and faults every queued waiter with <see cref="ObjectDisposedException"/>; idempotent.</summary>
    /// <remarks>The current holder, if any, is unaffected and can still release.</remarks>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        // Acquisitions read the flag under the lock, so none can queue once this lock section drains the queue.
        lock (_sync)
        {
            var waiter = _head;
            _head = null;
            _tail = null;
            while (waiter != null)
            {
                var next = waiter.Next;
                waiter.Detach();
                _ = waiter.TrySetException(new ObjectDisposedException(nameof(AsyncLock)));
                waiter = next;
            }
        }
    }

    /// <summary>Gets a value indicating whether the acquisition issued <paramref name="generation"/> still owns the lock.</summary>
    /// <param name="generation">The generation of an acquisition; only <see cref="AsyncLockOwnership"/> asks.</param>
    /// <returns>
    /// <see langword="true"/> only while the acquisition issued <paramref name="generation"/> has not released; zero (a
    /// <see langword="default"/> ownership) is never held.
    /// </returns>
    /// <remarks>
    /// Lock-free: the holder generation is one 64-bit field read atomically. It is written under <see cref="_sync"/>
    /// before the acquisition is published (the lock exit or the waiter completion), so the acquiring flow always reads
    /// its own generation, and its release is ordered before anything the same flow does next. Generations only grow,
    /// so a released or superseded acquisition never matches again.
    /// </remarks>
    internal bool IsHeldBy(ulong generation) => generation != 0 && Volatile.Read(ref _holderGeneration) == generation;

    /// <summary>Acquires the lock, waiting in FIFO order while it is held.</summary>
    /// <param name="cancellationToken">Cancels the wait while the acquisition is still queued.</param>
    /// <returns>The holder that releases the lock when disposed.</returns>
    /// <remarks>
    /// A disposed lock or a lock disposed while the acquisition is queued faults the result with
    /// <see cref="ObjectDisposedException"/>; a canceled wait faults it with <see cref="OperationCanceledException"/>.
    /// </remarks>
    internal ValueTask<AsyncLockHolder> LockAsync(CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            if (Volatile.Read(ref _disposed) != 0)
                return ValueTask.FromException<AsyncLockHolder>(new ObjectDisposedException(nameof(AsyncLock)));

            if (cancellationToken.IsCancellationRequested)
                return ValueTask.FromCanceled<AsyncLockHolder>(cancellationToken);

            if (TryTakeFree(out var generation))
                return new ValueTask<AsyncLockHolder>(new AsyncLockHolder(this, generation));

            var waiter = new Waiter(this);
            Enqueue(waiter);

            // Registering under the lock keeps the registration visible to whoever dequeues the node; a token canceled
            // in the meantime runs the callback inline, which re-enters the lock and removes the node right away.
            if (cancellationToken.CanBeCanceled)
                waiter.Registration = cancellationToken.UnsafeRegister(CancelWaiter, waiter);

            return new ValueTask<AsyncLockHolder>(waiter.Task);
        }
    }

    /// <summary>Hands ownership to the oldest queued waiter, or marks the lock free when none is queued.</summary>
    /// <remarks>
    /// Never throws; after <see cref="Dispose"/> the queue is empty, so it only marks the lock free. A hand-off issues the
    /// next generation before completing the waiter, so the releasing ownership stops matching <see cref="IsHeldBy"/> at once
    /// and the new holder observes its own generation when it resumes.
    /// </remarks>
    internal void Release()
    {
        lock (_sync)
        {
            while (_head != null)
            {
                var waiter = _head;
                Unlink(waiter);
                var generation = IssueGeneration();
                if (waiter.TrySetResult(new AsyncLockHolder(this, generation)))
                    return;
            }

            Volatile.Write(ref _holderGeneration, 0UL);
        }
    }

    /// <summary>Acquires the lock only when it is free.</summary>
    /// <param name="holder">The holder that releases the lock when disposed, or <see langword="default"/> when the lock is held.</param>
    /// <param name="cancellationToken">A canceled token refuses the acquisition with <see cref="OperationCanceledException"/>.</param>
    /// <returns><see langword="true"/> when the lock was acquired.</returns>
    internal bool TryLock(out AsyncLockHolder holder, CancellationToken cancellationToken)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            cancellationToken.ThrowIfCancellationRequested();
            if (TryTakeFree(out var generation))
            {
                holder = new AsyncLockHolder(this, generation);
                return true;
            }
        }

        holder = default;
        return false;
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

    /// <summary>Issues the next ownership generation and publishes it as the current holder's.</summary>
    /// <returns>The issued generation, never zero.</returns>
    /// <remarks>Callers hold <see cref="_sync"/>.</remarks>
    private ulong IssueGeneration()
    {
        var generation = ++_lastGeneration;
        Volatile.Write(ref _holderGeneration, generation);
        return generation;
    }

    /// <summary>Marks the lock held when it is free.</summary>
    /// <param name="generation">The generation issued to the caller, or zero when the lock is held.</param>
    /// <returns><see langword="true"/> when the lock was free and is now held by the caller.</returns>
    /// <remarks>Callers hold <see cref="_sync"/>.</remarks>
    private bool TryTakeFree(out ulong generation)
    {
        if (_holderGeneration != 0)
        {
            generation = 0;
            return false;
        }

        generation = IssueGeneration();
        return true;
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

    /// <summary>Queued acquisition: a completion source linked into the owner's FIFO queue, guarded by the owner's lock.</summary>
    private sealed class Waiter : TaskCompletionSource<AsyncLockHolder>
    {
        internal Waiter(AsyncLock owner)
            : base(TaskCreationOptions.RunContinuationsAsynchronously)
        {
            Owner = owner;
        }

        internal bool IsQueued { get; set; }

        internal Waiter? Next { get; set; }

        internal AsyncLock Owner { get; }

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
