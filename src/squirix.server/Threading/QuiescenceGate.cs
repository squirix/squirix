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
/// </remarks>
internal sealed class QuiescenceGate
{
    private readonly Lock _lock = new();

    private int _count;

    private TaskCompletionSource? _drained;

    internal bool HasPending
    {
        get
        {
            lock (_lock)
                return _count > 0;
        }
    }

    internal void Enter()
    {
        lock (_lock)
            _count++;
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
}
