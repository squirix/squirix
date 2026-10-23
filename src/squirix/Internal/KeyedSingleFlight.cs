using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;

namespace Squirix.Internal;

/// <summary>
/// Shares the produce step of concurrent callers per key, and the request that carries the produced value among the callers that joined
/// before that request was sent. A flight is published before its work starts and removed before its task completes, so a call made after a
/// flight finished always starts a new one.
/// </summary>
/// <remarks>
/// <list type="bullet">
/// <item>A caller that arrives after the request was sent reuses the produced value and sends its own request, so its reply is never older
/// than its call.</item>
/// <item>Each caller waits with its own token. The shared work runs with a token that is canceled only when every waiting caller has
/// canceled; a caller whose token cannot be canceled keeps the flight running to the end.</item>
/// </list>
/// </remarks>
/// <typeparam name="TValue">The value the produce step creates and the send step carries.</typeparam>
/// <typeparam name="TResult">The result the send step returns.</typeparam>
[Immutable]
internal sealed class KeyedSingleFlight<TValue, TResult>
{
    private readonly ConcurrentDictionary<string, Flight> _flights = new(StringComparer.Ordinal);

    private enum JoinOutcome
    {
        Joined = 0,
        Sent = 1,
        Ended = 2,
    }

    private enum Phase
    {
        Producing = 0,
        Sending = 1,
        Ended = 2,
    }

    /// <summary>Joins the running flight for the key, sends a request with its produced value, or starts a new flight.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="key">The flight key.</param>
    /// <param name="state">The caller state, passed to the steps this caller runs.</param>
    /// <param name="produce">Creates the value; runs once per flight.</param>
    /// <param name="send">Sends the request that carries the value; runs once per flight and once per caller that arrives after it was sent.</param>
    /// <param name="cancellationToken">The caller token; it stops this caller's wait and cancels the shared work only when no other caller waits.</param>
    /// <returns>The result of the request this caller waits for.</returns>
    internal Task<TResult> RunAsync<TState>(
        string key,
        TState state,
        Func<TState, CancellationToken, Task<TValue>> produce,
        Func<TState, TValue?, CancellationToken, Task<TResult>> send,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(produce);
        ArgumentNullException.ThrowIfNull(send);

        if (cancellationToken.IsCancellationRequested)
            return Task.FromCanceled<TResult>(cancellationToken);

        while (true)
        {
            if (!_flights.TryGetValue(key, out var running))
            {
                var flight = new Flight(cancellationToken.CanBeCanceled);
                if (_flights.TryAdd(key, flight))
                    return StartAsync(key, flight, state, produce, send, cancellationToken);

                continue;
            }

            var outcome = running.Join(out var produced);
            if (outcome == JoinOutcome.Joined)
                return WaitAsync(running, cancellationToken);

            if (outcome == JoinOutcome.Sent)
                return SendAsync(state, produced, send, cancellationToken);

            // The flight ended but is still published: it finished, or its last caller left while it was producing. A new flight takes its place.
            var replacement = new Flight(cancellationToken.CanBeCanceled);
            if (_flights.TryUpdate(key, replacement, running))
                return StartAsync(key, replacement, state, produce, send, cancellationToken);
        }
    }

    /// <summary>Waits for the flight with the caller token, and leaves the flight when that token cancels the wait.</summary>
    /// <param name="flight">The flight the caller joined or started.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The flight result, or a task canceled with the caller token.</returns>
    private static Task<TResult> WaitAsync(Flight flight, CancellationToken cancellationToken) =>
        cancellationToken.CanBeCanceled && !flight.Completion.IsCompleted ? WaitOrLeaveAsync(flight, cancellationToken) : flight.Completion;

    /// <summary>Awaits the flight until it completes or the caller token is canceled.</summary>
    /// <param name="flight">The flight the caller waits for.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The flight result.</returns>
    private static async Task<TResult> WaitOrLeaveAsync(Flight flight, CancellationToken cancellationToken)
    {
        try
        {
            return await flight.Completion.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            flight.Leave();
            throw;
        }
    }

    /// <summary>Sends the request of a caller that arrived after the flight sent its own, so a synchronous throw surfaces as a faulted task.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="state">The caller state.</param>
    /// <param name="produced">The value the flight produced.</param>
    /// <param name="send">The send step.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The result of the caller's own request.</returns>
#pragma warning disable ZA1101, MA0215 // The async state machine is what turns a synchronous throw into a faulted task.
    private static async Task<TResult> SendAsync<TState>(
        TState state,
        TValue? produced,
        Func<TState, TValue?, CancellationToken, Task<TResult>> send,
        CancellationToken cancellationToken) =>
        await send(state, produced, cancellationToken).ConfigureAwait(false);
#pragma warning restore ZA1101, MA0215

    /// <summary>Produces the value, marks the flight as sent and sends the request, all with the shared token of the flight.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="flight">The flight.</param>
    /// <param name="state">The state of the caller that started the flight.</param>
    /// <param name="produce">The produce step.</param>
    /// <param name="send">The send step.</param>
    /// <returns>The result of the shared request.</returns>
    /// <exception cref="OperationCanceledException">Every caller left the flight while it was producing.</exception>
    private static async Task<TResult> ProduceAndSendAsync<TState>(
        Flight flight,
        TState state,
        Func<TState, CancellationToken, Task<TValue>> produce,
        Func<TState, TValue?, CancellationToken, Task<TResult>> send)
    {
        var token = flight.Token;
        var produced = await produce(state, token).ConfigureAwait(false);

        // Every caller that joined before this point waits for a request sent after its call; later callers send their own.
        // A flight its last caller left while it was producing sends nothing, even when the factory ignored the cancel.
        return flight.TryMarkSending(produced) ? await send(state, produced, token).ConfigureAwait(false) : throw new OperationCanceledException(token);
    }

    /// <summary>Starts a published flight and returns the wait of the caller that started it.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="key">The flight key.</param>
    /// <param name="flight">The published flight.</param>
    /// <param name="state">The caller state.</param>
    /// <param name="produce">The produce step.</param>
    /// <param name="send">The send step.</param>
    /// <param name="cancellationToken">The caller token.</param>
    /// <returns>The wait of the caller that started the flight.</returns>
    private Task<TResult> StartAsync<TState>(
        string key,
        Flight flight,
        TState state,
        Func<TState, CancellationToken, Task<TValue>> produce,
        Func<TState, TValue?, CancellationToken, Task<TResult>> send,
        CancellationToken cancellationToken)
    {
        _ = DriveAsync(key, flight, state, produce, send);
        return WaitAsync(flight, cancellationToken);
    }

    /// <summary>Runs the flight on the calling thread until its first suspension, then completes it; never throws.</summary>
    /// <typeparam name="TState">The caller state type.</typeparam>
    /// <param name="key">The flight key.</param>
    /// <param name="flight">The published flight, removed from the map before it is completed.</param>
    /// <param name="state">The state of the caller that started the flight.</param>
    /// <param name="produce">The produce step.</param>
    /// <param name="send">The send step.</param>
    /// <returns>A task that completes once the flight has been completed.</returns>
    private async Task DriveAsync<TState>(
        string key,
        Flight flight,
        TState state,
        Func<TState, CancellationToken, Task<TValue>> produce,
        Func<TState, TValue?, CancellationToken, Task<TResult>> send)
    {
        var work = ProduceAndSendAsync(flight, state, produce, send);
        Task untyped = work;
        await untyped.ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);

        // Removal precedes completion so a continuation that calls RunAsync again starts a new flight.
        flight.End();
        Remove(key, flight);
        if (work.IsCanceled)
            flight.SetCanceled();
        else if (work.IsFaulted)
            flight.SetException(work.Exception.InnerExceptions);
        else
            flight.SetResult(await work.ConfigureAwait(false));
    }

    /// <summary>Removes the flight only while it is still the one published for the key, so a newer flight is never removed.</summary>
    /// <param name="key">The flight key.</param>
    /// <param name="flight">The finished flight.</param>
    private void Remove(string key, Flight flight) => _ = _flights.TryRemove(new KeyValuePair<string, Flight>(key, flight));

    /// <summary>The shared state of one flight: its phase, its waiting callers, its shared token and its result.</summary>
    [SuppressMessage("Design", "CA1001", Justification = "The flight disposes its token source itself, in End or after the cancel of its last caller.")]
    private sealed class Flight
    {
        private readonly Lock _gate = new();

        /// <summary>The source of the shared token; <see langword="null" /> when the caller that started the flight cannot leave it.</summary>
        private readonly CancellationTokenSource? _cancellation;

        private readonly TaskCompletionSource<TResult> _completion = new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>The number of callers that wait for the shared request; guarded by <see cref="_gate" />.</summary>
        private int _callers = 1;

        /// <summary>Whether the last caller is canceling the shared token; guarded by <see cref="_gate" />.</summary>
        private bool _canceling;

        /// <summary>Whether the work finished and <see cref="End" /> ran; guarded by <see cref="_gate" />.</summary>
        private bool _ended;

        /// <summary>The phase of the flight; guarded by <see cref="_gate" />.</summary>
        private Phase _phase;

        /// <summary>The produced value, set when the flight sends its request; guarded by <see cref="_gate" />.</summary>
        private TValue? _produced;

        /// <summary>Initializes a new instance of the <see cref="Flight" /> class.</summary>
        /// <param name="canBeCanceled">Whether the caller that starts the flight can leave it; when not, the flight always runs to the end.</param>
        internal Flight(bool canBeCanceled)
        {
            if (!canBeCanceled)
                return;

#pragma warning disable ZA1102 // Disposed by End, or by the last caller to leave when End runs during its cancel.
            _cancellation = new CancellationTokenSource();
#pragma warning restore ZA1102
            Token = _cancellation.Token;
        }

        /// <summary>Gets the token of the shared work; kept so it stays readable after the source is disposed.</summary>
        internal CancellationToken Token { get; }

        /// <summary>Gets the task every waiting caller observes.</summary>
        [SuppressMessage("Usage", "VSTHRD003", Justification = "The flight creates this task and completes it only from its own work.")]
        internal Task<TResult> Completion => _completion.Task;

        /// <summary>Joins the flight while it is producing, or hands out the produced value once its request was sent.</summary>
        /// <param name="produced">The produced value when the outcome is <see cref="JoinOutcome.Sent" />.</param>
        /// <returns>The join outcome.</returns>
        internal JoinOutcome Join(out TValue? produced)
        {
            lock (_gate)
            {
                produced = _produced;
                if (_phase != Phase.Producing)
                    return _phase == Phase.Sending ? JoinOutcome.Sent : JoinOutcome.Ended;

                _callers++;
                return JoinOutcome.Joined;
            }
        }

        /// <summary>Records the produced value and closes the flight to new callers, unless every caller left while it was producing.</summary>
        /// <param name="produced">The produced value.</param>
        /// <returns><see langword="true" /> when the flight goes on to send its request.</returns>
        internal bool TryMarkSending(TValue produced)
        {
            lock (_gate)
            {
                if (_phase != Phase.Producing)
                    return false;

                _produced = produced;
                _phase = Phase.Sending;
                return true;
            }
        }

        /// <summary>Removes a caller whose token canceled its wait; the last caller to leave cancels the shared work.</summary>
        internal void Leave()
        {
            lock (_gate)
            {
                if (_phase == Phase.Ended || --_callers > 0 || _cancellation == null)
                    return;

                // A producing flight nobody waits for must not take new callers: they would wait for work that is being canceled.
                if (_phase == Phase.Producing)
                    _phase = Phase.Ended;

                _canceling = true;
            }

            // Callbacks run on this thread, as for any token the work was given; the work may even end the flight from inside them.
            try
            {
#pragma warning disable MA0045
                _cancellation.Cancel();
#pragma warning restore MA0045
            }
#pragma warning disable ERP022 // Nobody waits for the shared work any more; the leaving caller must get the cancel of its own token.
            catch (AggregateException)
            {
                // A callback the shared work registered on the token failed while it was canceled.
            }
#pragma warning restore ERP022
            finally
            {
                bool dispose;
                lock (_gate)
                {
                    // The work ended during the cancel, and End left the dispose to this caller.
                    _canceling = false;
                    dispose = _ended;
                }

                if (dispose)
                    _cancellation.Dispose();
            }
        }

        /// <summary>Closes the flight before it is removed and completed, and disposes the token source unless a cancel is running.</summary>
        internal void End()
        {
            lock (_gate)
            {
                _phase = Phase.Ended;
                _ended = true;

                // A cancel running now disposes the source once its callbacks return.
                if (_canceling)
                    return;
            }

            _cancellation?.Dispose();
        }

        /// <summary>Completes the flight as canceled by its shared token.</summary>
        internal void SetCanceled() => _ = _completion.TrySetCanceled(Token);

        /// <summary>Completes the flight with the failure of its work.</summary>
        /// <param name="exceptions">The failures.</param>
        internal void SetException(IEnumerable<Exception> exceptions) => _ = _completion.TrySetException(exceptions);

        /// <summary>Completes the flight with its result.</summary>
        /// <param name="result">The result.</param>
        internal void SetResult(TResult result) => _ = _completion.TrySetResult(result);
    }
}
