using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Guards the shared pending probe the stalled-gate tests rely on.</summary>
[Immutable]
public sealed class PendingProbeTests
{
    /// <summary>An operation nobody completes is reported pending.</summary>
    [Test]
    public async Task UncompletedOperationStaysPending()
    {
        var operation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        _ = await Assert.That(await PendingProbe.StaysPendingAsync(operation.Task)).IsTrue();
    }

    /// <summary>A completed operation is reported completed at once.</summary>
    [Test]
    public async Task CompletedOperationIsNotPending() => _ = await Assert.That(await PendingProbe.StaysPendingAsync(Task.CompletedTask)).IsFalse();

    /// <summary>The probe allows queued completion to run before reporting the operation as pending.</summary>
    [Test]
    public async Task QueuedCompletionIsObserved()
    {
        var previous = SynchronizationContext.Current;
        var context = new QueuedContext();
        var operation = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Task<bool> result;
        SynchronizationContext.SetSynchronizationContext(context);
        try
        {
            context.Post(_ => operation.SetResult(), null);
            result = PendingProbe.StaysPendingAsync(operation.Task);
            context.Drain();
        }
        finally
        {
            SynchronizationContext.SetSynchronizationContext(previous);
        }

        _ = await Assert.That(await result).IsFalse();
    }

    /// <summary>A FIFO context that executes posted work only when explicitly drained.</summary>
    private sealed class QueuedContext : SynchronizationContext
    {
        private readonly Queue<(SendOrPostCallback Callback, object? State)> _work = new();

        /// <inheritdoc />
        public override void Post(SendOrPostCallback d, object? state) => _work.Enqueue((d, state));

        /// <summary>Runs posted callbacks in their original order.</summary>
        internal void Drain()
        {
            while (_work.TryDequeue(out var item))
                item.Callback(item.State);
        }
    }
}
