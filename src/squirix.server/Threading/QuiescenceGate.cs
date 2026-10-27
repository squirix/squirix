using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Threading;

/// <summary>Coordinates in-flight operations that must drain before a barrier can proceed, isolating the bookkeeping from the consumer surface.</summary>
/// <remarks>
/// Counting quiescence gate: <see cref="Enter"/> and <see cref="Exit"/> track an in-flight
/// counter, and <see cref="WaitAsync"/> blocks until it drains to zero, establishing a quiescent
/// point before a barrier (snapshot cut, reclamation) proceeds. The pattern mirrors a grace period
/// in Read-Copy-Update, where a writer waits until all in-flight readers have finished.
/// Further reading:
/// <see href="https://lwn.net/Articles/262464/">What is RCU, Fundamentally? (LWN)</see>;
/// <see href="https://en.wikipedia.org/wiki/Read-copy-update">Read-copy-update (Wikipedia)</see>;
/// <see href="https://github.com/StephenCleary/AsyncEx/blob/master/src/Nito.AsyncEx.Coordination/AsyncCountdownEvent.cs">Nito.AsyncEx.AsyncCountdownEvent</see>;
/// <see href="https://devblogs.microsoft.com/dotnet/building-async-coordination-primitives-part-4-asyncbarrier/">Building Async Coordination Primitives (Stephen Toub)</see>;
/// <see href="https://dotnet.github.io/dotNext/api/DotNext.Threading.AsyncCountdownEvent.html">.NEXT AsyncCountdownEvent</see>.
/// <para>
/// Admission control keeps a barrier from starving under sustained load: while the gate is closed (<see cref="Close"/>), <see cref="TryEnter"/>
/// refuses new operations, so the in-flight counter can only fall and <see cref="WaitAsync"/> completes once the operations already admitted
/// have left. <see cref="Enter"/> stays unconditional for callers that already hold the barrier's own exclusion.
/// </para>
/// </remarks>
internal sealed class QuiescenceGate
{
    private readonly Lock _lock = new();

    private int _closers;

    private int _count;

    private TaskCompletionSource? _drained;

    private TaskCompletionSource? _opened;

    internal bool HasPending
    {
        get
        {
            lock (_lock)
                return _count > 0;
        }
    }

    /// <summary>Gets the number of in-flight operations.</summary>
    internal int Pending
    {
        get
        {
            lock (_lock)
                return _count;
        }
    }

    /// <summary>Refuses <see cref="TryEnter"/> until the matching <see cref="Open"/>; closings nest, so the gate reopens when every closer has opened it.</summary>
    internal void Close()
    {
        lock (_lock)
            _closers++;
    }

    /// <summary>Tracks one more in-flight operation regardless of <see cref="Close"/>.</summary>
    internal void Enter()
    {
        lock (_lock)
            _count++;
    }

    /// <summary>Withdraws one <see cref="Close"/> and wakes <see cref="WaitOpenAsync"/> waiters once the gate is open again.</summary>
    /// <exception cref="InvalidOperationException">The gate is not closed.</exception>
    internal void Open()
    {
        TaskCompletionSource? opened = null;
        lock (_lock)
        {
            if (_closers <= 0)
                throw new InvalidOperationException("The gate is not closed.");

            _closers--;
            if (_closers == 0)
            {
                opened = _opened;
                _opened = null;
            }
        }

        opened?.SetResult();
    }

    /// <summary>Tracks one more in-flight operation unless the gate is closed.</summary>
    /// <returns><see langword="true"/> when the operation was admitted and must <see cref="Exit"/>; <see langword="false"/> while the gate is closed.</returns>
    internal bool TryEnter()
    {
        lock (_lock)
        {
            if (_closers > 0)
                return false;

            _count++;
            return true;
        }
    }

    internal void Exit()
    {
        TaskCompletionSource? drained = null;
        lock (_lock)
        {
            if (_count <= 0)
                throw new InvalidOperationException("No pending operation is tracked.");

            _count--;
            if (_count == 0)
            {
                drained = _drained;
                _drained = null;
            }
        }

        drained?.SetResult();
    }

    internal ValueTask WaitAsync(CancellationToken cancellationToken)
    {
        Task task;
        lock (_lock)
        {
            if (_count == 0)
                return ValueTask.CompletedTask;

            _drained ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            task = _drained.Task;
        }

        return new ValueTask(task.WaitAsync(cancellationToken));
    }

    /// <summary>Waits until the gate is open.</summary>
    /// <param name="cancellationToken">Cancels the wait.</param>
    /// <returns>A task that completes when no <see cref="Close"/> is outstanding.</returns>
    internal ValueTask WaitOpenAsync(CancellationToken cancellationToken)
    {
        Task task;
        lock (_lock)
        {
            if (_closers == 0)
                return ValueTask.CompletedTask;

            _opened ??= new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            task = _opened.Task;
        }

        return new ValueTask(task.WaitAsync(cancellationToken));
    }
}
