using System;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.UnitTests.Support;

/// <summary>
/// A blocking body running on its own long-running thread, with its state passed explicitly. Disposal releases a body that still runs and waits
/// for it, so the thread is done with the state before the caller disposes the state.
/// </summary>
/// <typeparam name="TState">The state type.</typeparam>
internal sealed class DedicatedThread<TState> : IAsyncDisposable
{
    private const int JoinTimeoutSeconds = 10;
    private readonly TState _state;
    private readonly Action<TState> _body;
    private readonly Action<TState> _release;

    /// <summary>Initializes a new instance of the <see cref="DedicatedThread{TState}" /> class and starts the body.</summary>
    /// <param name="state">The state passed to the body and to the release.</param>
    /// <param name="body">The body, static so that it captures nothing.</param>
    /// <param name="release">Ends a body that still runs at disposal, static so that it captures nothing.</param>
    /// <param name="cancellationToken">The token that cancels the start of the body.</param>
    internal DedicatedThread(TState state, Action<TState> body, Action<TState> release, CancellationToken cancellationToken)
    {
        _state = state;
        _body = body;
        _release = release;
        Completion = Task.Factory.StartNew(static thread => Run(thread), this, cancellationToken, TaskCreationOptions.LongRunning, TaskScheduler.Default);
    }

    /// <summary>Gets the task that completes when the body returned.</summary>
    internal Task Completion { get; }

    /// <summary>Releases a body that still runs, then waits a bounded time for it to return.</summary>
    /// <returns>A task that completes when the body returned or the wait gave up.</returns>
    public async ValueTask DisposeAsync()
    {
        if (!Completion.IsCompleted)
            _release(_state);

        await Completion.WaitAsync(TimeSpan.FromSeconds(JoinTimeoutSeconds), TimeProvider.System).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    /// <summary>Runs the body of a thread.</summary>
    /// <param name="thread">The <see cref="DedicatedThread{TState}" /> to run.</param>
    /// <exception cref="InvalidOperationException">Thrown when <paramref name="thread" /> is not a <see cref="DedicatedThread{TState}" />.</exception>
    private static void Run(object? thread)
    {
        if (thread is not DedicatedThread<TState> started)
            throw new InvalidOperationException("Unexpected state.");

        started._body(started._state);
    }
}
