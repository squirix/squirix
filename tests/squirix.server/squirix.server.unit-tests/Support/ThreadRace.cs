using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// Runs race bodies on dedicated threads, all released by one barrier so they overlap. Race bodies spin without yielding, so they must not tie up
/// thread-pool threads other tests need.
/// </summary>
internal static class ThreadRace
{
    /// <summary>Runs every body on its own long-running thread and fails instead of hanging when they do not finish in time.</summary>
    /// <param name="bodies">The bodies to run at once; one whose awaits complete synchronously runs on its thread to the end.</param>
    /// <param name="timeout">How long to wait for every body.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when every body completed.</returns>
    internal static async Task RunAsync(IReadOnlyList<Func<Task>> bodies, TimeSpan timeout, CancellationToken cancellationToken)
    {
        using var barrier = new Barrier(bodies.Count);
        var jobs = new Task[bodies.Count];
        for (var i = 0; i < bodies.Count; i++)
        {
            jobs[i] = Task.Factory.StartNew(
                static state => RunAfterBarrierAsync(state),
                new GatedBody(barrier, bodies[i], cancellationToken),
                cancellationToken,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default).Unwrap();
        }

        await Task.WhenAll(jobs).WaitAsync(timeout, TimeProvider.System, cancellationToken);
    }

    /// <summary>Starts a blocking body on its own long-running thread; disposing the result releases the body and waits for it.</summary>
    /// <typeparam name="TState">The state type; a value tuple carries several values.</typeparam>
    /// <param name="state">The state passed to the body and to the release.</param>
    /// <param name="body">The body, static so that it captures nothing.</param>
    /// <param name="release">Ends a body that still runs at disposal, static so that it captures nothing.</param>
    /// <param name="cancellationToken">The token that cancels the start of the body.</param>
    /// <returns>The running thread; dispose it before the state.</returns>
    internal static DedicatedThread<TState> Start<TState>(TState state, Action<TState> body, Action<TState> release, CancellationToken cancellationToken) =>
        new(state, body, release, cancellationToken);

    /// <summary>Waits at the barrier, then starts the body on this thread.</summary>
    /// <param name="state">The <see cref="GatedBody" /> to run.</param>
    /// <returns>The task of the body.</returns>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="state" /> is not a <see cref="GatedBody" />.</exception>
    private static Task RunAfterBarrierAsync(object? state)
    {
        if (state is not GatedBody gated)
            throw new InvalidOperationException("Unexpected state.");

        gated.Barrier.SignalAndWait(gated.CancellationToken);
        return gated.Body();
    }

    [Immutable]
    private sealed class GatedBody
    {
        internal GatedBody(Barrier barrier, Func<Task> body, CancellationToken cancellationToken)
        {
            Barrier = barrier;
            Body = body;
            CancellationToken = cancellationToken;
        }

        internal Barrier Barrier { get; }

        internal Func<Task> Body { get; }

        internal CancellationToken CancellationToken { get; }
    }
}
