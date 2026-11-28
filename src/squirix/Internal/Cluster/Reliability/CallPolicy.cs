using System;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport;
using Squirix.Transport.Wire;

namespace Squirix.Internal.Cluster.Reliability;

internal sealed class CallPolicy : ICallPolicy
{
    private readonly ActiveOperationCounter _activeOperations = new();
    private readonly Lock _disposeGate = new();
    private readonly CallPolicyExecutor _executor;
    private readonly string _peer;
    private readonly int _capacity;
    private int _available;
    private SemaphoreSlim? _semaphore;
    private Task? _disposeTask;
    private TaskCompletionSource<bool>? _disposeTcs;
    private int _disposed;
    private volatile bool _draining;
    private bool _semaphoreDisposed;

    internal CallPolicy(
        TimeProvider timeProvider,
        TimeSpan? timeoutPerAttempt = null,
        int maxAttempts = 3,
        TimeSpan? baseBackoff = null,
        TimeSpan? maxBackoff = null,
        int maxConcurrentPerPeer = 256,
        string? peer = null)
    {
        _peer = string.IsNullOrWhiteSpace(peer) ? "unknown" : peer;
        var cap = Math.Max(1, maxConcurrentPerPeer);
        _capacity = cap;
        _available = cap;
        var settings = new CallPolicySettings(
            _peer,
            Math.Max(1, maxAttempts),
            timeoutPerAttempt ?? TimeSpan.FromMilliseconds(600),
            baseBackoff ?? TimeSpan.FromMilliseconds(50),
            maxBackoff ?? TimeSpan.FromMilliseconds(500));
        _executor = new CallPolicyExecutor(settings, timeProvider, this);
    }

    public void BeginDrain() => _draining = true;

    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
        {
            if (_disposeTask != null)
                return new ValueTask(_disposeTask);

            _draining = true;
            _ = Interlocked.Exchange(ref _disposed, 1);
            if (_activeOperations.IsIdle())
            {
                DisposeSemaphoreUnderLockIfIdle();
                _disposeTask = Task.CompletedTask;
            }
            else
            {
                _disposeTcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _disposeTask = _disposeTcs.Task;
            }

            return new ValueTask(_disposeTask);
        }
    }

    public ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken, DeadlineBudget? budget = null, Action<SquirixTransportException>? onFailure = null) =>
        _executor.RunQueuedExecutionAsync(static (call, token, _) => call.Action(call.State, token), (Action: action, State: state), budget, onFailure, false, cancellationToken);

    public ValueTask<T> ExecuteTransportAsync<TState, T>(Func<TState, CancellationToken, long, ValueTask<T>> action, TState state, DeadlineBudget budget, Action<SquirixTransportException>? onFailure, CancellationToken cancellationToken) =>
        _executor.RunQueuedExecutionAsync(action, state, budget, onFailure, true, cancellationToken);

    private void CheckCanEnter(CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        ThrowIfDraining();
        cancellationToken.ThrowIfCancellationRequested();
    }

    private void DisposeSemaphoreUnderLockIfIdle()
    {
        if (Volatile.Read(ref _disposed) == 0 || _semaphoreDisposed || !_activeOperations.IsIdle())
            return;

        _semaphore?.Dispose();
        _semaphoreDisposed = true;
        _ = _disposeTcs?.TrySetResult(true);
    }

    private Task<bool> TryAdmitAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return TryDirectGrant() ? Task.FromResult(true) : _semaphore!.WaitAsync(0, token);
    }

    private Task AdmitAsync(CancellationToken token)
    {
        token.ThrowIfCancellationRequested();
        return TryDirectGrant() ? Task.CompletedTask : _semaphore!.WaitAsync(token);
    }

    private bool TryDirectGrant()
    {
        var available = Volatile.Read(ref _available);
        while (available >= 0)
        {
            if (available == 0 && Volatile.Read(ref _semaphore) == null)
            {
                var queue = new SemaphoreSlim(0, _capacity);
                if (Interlocked.CompareExchange(ref _semaphore, queue, null) != null)
                    queue.Dispose();
            }

            var seen = Interlocked.CompareExchange(ref _available, available - 1, available);
            if (seen == available)
                return available != 0;
            available = seen;
        }

        return false;
    }

    private void ReleasePermit()
    {
        var available = Volatile.Read(ref _available);
        while (available >= 0)
        {
            var seen = Interlocked.CompareExchange(ref _available, available + 1, available);
            if (seen == available)
                return;
            available = seen;
        }

        // After the first saturation, every grant is returned to the same bounded waiter queue.
        _ = _semaphore!.Release();
    }

    private void ReleaseActiveOperation()
    {
        if (!_activeOperations.TryExitToIdle() || Volatile.Read(ref _disposed) == 0)
            return;

        // A concurrent disposer either observes the idle counter itself or publishes disposal before this check.
        lock (_disposeGate)
            DisposeSemaphoreUnderLockIfIdle();
    }

    private void ThrowIfDraining()
    {
        if (!_draining)
            return;

        CallPolicyMetrics.IncrementDrainRejectsTotal(_peer, 1);
        throw new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unavailable, "Peer client pool is draining."));
    }

    private sealed class ActiveOperationCounter
    {
        private int _count;

        internal void Enter() => _ = Interlocked.Increment(ref _count);

        internal bool IsIdle() => Volatile.Read(ref _count) == 0;

        /// <summary>Decrements the counter.</summary>
        /// <returns><see langword="true" /> when the count reaches zero; otherwise <see langword="false" />.</returns>
        internal bool TryExitToIdle() => Interlocked.Decrement(ref _count) == 0;
    }

    [Immutable]
    private sealed class CallPolicyExecutor
    {
        private readonly TimeSpan _baseBackoff;
        private readonly CallPolicy _owner;
        private readonly int _maxAttempts;
        private readonly TimeSpan _maxBackoff;
        private readonly string _peer;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _timeoutPerAttempt;

        internal CallPolicyExecutor(CallPolicySettings settings, TimeProvider timeProvider, CallPolicy owner)
        {
            _peer = settings.Peer;
            _maxAttempts = settings.MaxAttempts;
            _timeoutPerAttempt = settings.TimeoutPerAttempt;
            _baseBackoff = settings.BaseBackoff;
            _maxBackoff = settings.MaxBackoff;
            _timeProvider = timeProvider;
            _owner = owner;
        }

        [AsyncMethodBuilder(typeof(PolicyMethodBuilder<>))]
        internal async ValueTask<T> RunQueuedExecutionAsync<TState, T>(
            Func<TState, CancellationToken, long, ValueTask<T>> action,
            TState state,
            DeadlineBudget? budget,
            Action<SquirixTransportException>? onFailure,
            bool nativeTransport,
            CancellationToken cancellationToken)
        {
            try
            {
                using var operation = new ActiveOperation(_owner);
                _owner.CheckCanEnter(cancellationToken);
                using var budgetScope = CreateIndependentBudget(budget, cancellationToken);
                var effectiveToken = budgetScope?.Token ?? cancellationToken;
                var queueWaitStarted = CallPolicyMetrics.StartQueueWait();
                var sharedClock = budget is { } value && UsesClock(value.Clock);
                var acquired = sharedClock && await _owner.TryAdmitAsync(effectiveToken).ConfigureAwait(false);
                using var queuedScope = CreateQueuedScope(acquired, budget, effectiveToken);
                effectiveToken = queuedScope?.Token ?? effectiveToken;

                if (!acquired)
                    await WaitForSlotAsync(budget, effectiveToken, cancellationToken).ConfigureAwait(false);

                CallPolicyMetrics.FinishQueueWait(_peer, queueWaitStarted);
                try
                {
                    _owner.ThrowIfDraining();
                    var attempt = 0;
                    Exception? last = null;

                    while (OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts && HasAttemptBudget(budget, sharedClock, out var remaining))
                    {
                        attempt++;
                        var attemptScope = CreateAttemptScope(remaining, sharedClock && effectiveToken == cancellationToken, nativeTransport, effectiveToken, out var attemptBoundsBudget, out var expires);
                        var attemptToken = attemptScope?.Token ?? effectiveToken;
                        Exception failure;
                        try
                        {
                            return await action(state, attemptToken, expires).ConfigureAwait(false);
                        }
                        catch (Exception ex) when (ex is OperationCanceledException or SquirixTransportException or HttpRequestException)
                        {
                            failure = ex;
                        }
                        finally
                        {
                            attemptScope?.Dispose();
                        }

                        var outcome = await MapFailedAttemptAsync(failure, attemptBoundsBudget, attempt, budget, effectiveToken, cancellationToken, BinaryCallAlarm.FailureToken(expires, failure, attemptToken)).ConfigureAwait(false);
                        last = outcome.LastException;
                        if (!outcome.ShouldRetry)
                            break;
                    }

                    return FinishFailedAttempts<T>(last, budget, effectiveToken, cancellationToken);
                }
                finally
                {
                    _owner.ReleasePermit();
                }
            }
            catch (SquirixTransportException error) when (onFailure != null)
            {
                onFailure(error);
                throw;
            }
        }

        private static bool HasAttemptBudget(DeadlineBudget? budget, bool sharedClock, out TimeSpan? remaining)
        {
            remaining = budget?.Remaining;
            return !sharedClock || remaining > TimeSpan.Zero;
        }

        private static Exception MapLastAttemptFailure(Exception? last) => last switch
        {
            OperationCanceledException => new SquirixTransportException(new CallStatus(CacheTransportStatusCode.DeadlineExceeded, "All attempts timed out.")),
            SquirixTransportException { StatusCode: CacheTransportStatusCode.Cancelled } => new SquirixTransportException(new CallStatus(CacheTransportStatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout.")),
            _ => last!,
        };

        private static bool ShouldUseEffectiveTokenDirectly(TimeSpan? budgetRemaining, TimeSpan perAttempt) => budgetRemaining != null && perAttempt >= budgetRemaining.Value;

        private async Task WaitForSlotAsync(DeadlineBudget? budget, CancellationToken effectiveToken, CancellationToken cancellationToken)
        {
            try
            {
                await _owner.AdmitAsync(effectiveToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget != null && !cancellationToken.IsCancellationRequested)
            {
                // The operation deadline can expire while queued on the per-peer semaphore. Surface it as
                // the same deadline-budget SquirixTransportException the retry loop produces instead of leaking a raw
                // OperationCanceledException.
                throw CreateBudgetExceeded();
            }
        }

        private TimedCancellation? CreateAttemptScope(TimeSpan? remainingBudget, bool sharedClockBudget, bool nativeTransport, CancellationToken effectiveToken, out bool attemptBoundsBudget, out long expires)
        {
            var remaining = GetAttemptTimeoutForRemaining(remainingBudget);

            attemptBoundsBudget = sharedClockBudget && remainingBudget is { } bound && remaining >= bound;
            expires = nativeTransport && sharedClockBudget && UsesClock(TimeProvider.System) && remaining >= TimeSpan.Zero && remaining.TotalMilliseconds <= uint.MaxValue - 1
                ? BinaryCallAlarm.DueAfter(remaining)
                : 0;

            // Queued calls retain an operation-wide token; uncontended shared-clock calls use one timer for both limits.
            return expires != 0 || (!sharedClockBudget && ShouldUseEffectiveTokenDirectly(remainingBudget, remaining))
                ? null
                : new TimedCancellation(remaining, _timeProvider, effectiveToken);
        }

        private bool IsSharedBudgetSpent(DeadlineBudget? budget) => budget is { } value && UsesClock(value.Clock) && value.Remaining <= TimeSpan.Zero;

        private TimedCancellation? CreateQueuedScope(bool acquired, DeadlineBudget? budget, CancellationToken cancellationToken)
        {
            if (acquired || budget is not { } value || !UsesClock(value.Clock))
                return null;

            var remaining = value.Remaining;
            return new TimedCancellation(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero, _timeProvider, cancellationToken);
        }

        private Task BackoffAsync(TimeSpan span, CancellationToken cancellationToken)
        {
            CallPolicyMetrics.IncrementBackoffLabel(_peer, 1);
            CallPolicyMetrics.ObserveBackoffSeconds(_peer, span);
            return Task.Delay(span, _timeProvider, cancellationToken);
        }

        private async Task<Exception> BackoffOrCaptureCancellationAsync(TimeSpan delay, Exception last, DeadlineBudget? budget, CancellationToken outerCt)
        {
            // A same-clock delay can consume at most the remaining budget; the retry loop checks it again afterwards.
            var exhaustsBudget = false;
            if (budget is { } value && UsesClock(value.Clock))
            {
                var remaining = value.Remaining;
                if (remaining <= delay)
                {
                    exhaustsBudget = true;
                    delay = remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero;
                }
            }

            try
            {
                await BackoffAsync(delay, outerCt).ConfigureAwait(false);
                if (!exhaustsBudget)
                    return last;
                outerCt.ThrowIfCancellationRequested();
                throw CreateBudgetExceeded();
            }
            catch (OperationCanceledException oce) when (outerCt.IsCancellationRequested)
            {
                return oce;
            }
        }

        private TimeSpan BackoffWithJitter(int attempt) => CallPolicyDefaults.RetryDelay(attempt, _baseBackoff, _maxBackoff);

        private TimeSpan GetAttemptTimeoutForRemaining(TimeSpan? remaining)
        {
            return remaining switch
            {
                null => _timeoutPerAttempt,
                { } value when value <= TimeSpan.Zero => TimeSpan.Zero,
                { } value when _timeoutPerAttempt == Timeout.InfiniteTimeSpan => value,
                { } value => value < _timeoutPerAttempt ? value : _timeoutPerAttempt,
            };
        }

        private SquirixTransportException CreateBudgetExceeded()
        {
            RpcTimeoutMetrics.TimeoutsTotal.Add(_peer, "overall", "deadline_budget");
            return new SquirixTransportException(new CallStatus(CacheTransportStatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private async ValueTask<AttemptFailure> MapHttpFailureAsync(HttpRequestException ex, int attempt, DeadlineBudget? budget, CancellationToken effectiveToken)
        {
            if (ConnectionRejectedException.Map(ex) is { } rejected)
                return AttemptFailure.Stop(rejected);

            if (attempt >= _maxAttempts || !OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) || IsSharedBudgetSpent(budget))
                return AttemptFailure.Stop(ex);

            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(ex));
            return AttemptFailure.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), ex, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptFailure> MapOperationCanceledFailureAsync(
            OperationCanceledException oce,
            int attempt,
            DeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken,
            CancellationToken attemptToken)
        {
            if (!cancellationToken.IsCancellationRequested && IsSharedBudgetSpent(budget))
                return AttemptFailure.Stop(oce);

            var cancelKind = OperationCancellationClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptToken);
            if (cancelKind != CancellationScenarioKind.PerAttemptTimedOut || attempt >= _maxAttempts)
                return AttemptFailure.Stop(oce);

            RpcTimeoutMetrics.TimeoutsTotal.Add(_peer, "attempt", "operation_canceled");
            CallPolicyMetrics.IncrementRetriesTotal(_peer, "operation_canceled");
            return AttemptFailure.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), oce, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptFailure> MapRpcFailureAsync(SquirixTransportException rx, int attempt, DeadlineBudget? budget, CancellationToken effectiveToken, CancellationToken cancellationToken)
        {
            if (ConnectionRejectedException.Map(rx) is { } rejected)
                return AttemptFailure.Stop(rejected);

            // A mutation may have committed durably even though its outcome is unknown (server write-ahead
            // idempotency intent). Re-sending the same operation would not re-execute (the server gates on the
            // intent), but the outcome is still ambiguous: stop retrying and surface it to the caller.
            if (rx.Status.PayloadTooLarge || CommitOutcomeUnknownDetail.Matches(rx.Status.Detail))
                return AttemptFailure.Stop(rx);

            // The transport deadline timer and the budget timer expire at the same instant: when the server reports the
            // expiry first, it is the exhausted operation budget, not a per-attempt timeout to retry.
            if (rx.StatusCode is CacheTransportStatusCode.Cancelled or CacheTransportStatusCode.DeadlineExceeded && !cancellationToken.IsCancellationRequested &&
                budget?.Remaining is { } remainingBudget && remainingBudget <= TimeSpan.Zero)
                throw CreateBudgetExceeded();

            var canRetry = attempt < _maxAttempts && OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget);
            if (!canRetry)
                return AttemptFailure.Stop(rx);

            if (rx.StatusCode == CacheTransportStatusCode.Cancelled || rx.StatusCode == CacheTransportStatusCode.DeadlineExceeded)
            {
                var reason = rx.StatusCode == CacheTransportStatusCode.DeadlineExceeded ? CallPolicyRetryClassifier.DeadlineExceeded : CallPolicyRetryClassifier.Canceled;
                RpcTimeoutMetrics.TimeoutsTotal.Add(_peer, "attempt", reason);
                CallPolicyMetrics.IncrementRetriesTotal(_peer, reason);
                return AttemptFailure.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
            }

            if (rx.StatusCode is not (CacheTransportStatusCode.Unavailable or CacheTransportStatusCode.Internal or CacheTransportStatusCode.ResourceExhausted))
                return AttemptFailure.Stop(rx);
            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(rx));
            return AttemptFailure.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
        }

        private T FinishFailedAttempts<T>(Exception? last, DeadlineBudget? budget, CancellationToken effectiveToken, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();

            // An ambiguous durable outcome must retain its no-retry contract even at the deadline boundary.
            if (last is SquirixTransportException rpc && (rpc.Status.PayloadTooLarge || CommitOutcomeUnknownDetail.Matches(rpc.Status.Detail)))
                throw rpc;

            if (budget == null || (OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget)))
                throw MapLastAttemptFailure(last);

            throw CreateBudgetExceeded();
        }

        private bool UsesClock(TimeProvider clock) => ReferenceEquals(_timeProvider, clock);

        private TimedCancellation? CreateIndependentBudget(DeadlineBudget? budget, CancellationToken token)
        {
            if (budget is not { } value || UsesClock(value.Clock))
                return null;
            var remaining = value.Remaining;
            return new TimedCancellation(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero, value.Clock, token);
        }

        private async ValueTask<AttemptFailure> MapFailedAttemptAsync(
            Exception failure, bool attemptBoundsBudget, int attempt, DeadlineBudget? deadlineBudget, CancellationToken effectiveToken, CancellationToken cancellationToken, CancellationToken attemptToken)
        {
            // The timer records which limit won even if its callback precedes the next timestamp observation.
            return failure switch
            {
                SquirixTransportException rpc when rpc.Status.PayloadTooLarge || CommitOutcomeUnknownDetail.Matches(rpc.Status.Detail) => AttemptFailure.Stop(rpc),
                _ when attemptBoundsBudget && !cancellationToken.IsCancellationRequested &&
                    (attemptToken.IsCancellationRequested || failure is SquirixTransportException { ConnectAttemptTimedOut: true }) => throw CreateBudgetExceeded(),
                OperationCanceledException oce => await MapOperationCanceledFailureAsync(oce, attempt, deadlineBudget, effectiveToken, cancellationToken, attemptToken).ConfigureAwait(false),
                SquirixTransportException rx => await MapRpcFailureAsync(rx, attempt, deadlineBudget, effectiveToken, cancellationToken).ConfigureAwait(false),
                HttpRequestException http => await MapHttpFailureAsync(http, attempt, deadlineBudget, effectiveToken).ConfigureAwait(false),
                _ => throw new InvalidOperationException("Unreachable attempt failure classification."),
            };
        }

        private readonly struct ActiveOperation : IDisposable
        {
            private readonly CallPolicy _owner;

            internal ActiveOperation(CallPolicy owner)
            {
                _owner = owner;
                owner._activeOperations.Enter();
            }

            public void Dispose() => _owner.ReleaseActiveOperation();
        }

        [Immutable]
        private readonly record struct AttemptFailure(Exception LastException, bool ShouldRetry)
        {
            internal static AttemptFailure Retry(Exception error) => new(error, true);

            internal static AttemptFailure Stop(Exception error) => new(error, false);
        }

        private static class CallPolicyRetryClassifier
        {
            internal const string Canceled = "canceled";
            internal const string DeadlineExceeded = "deadline_exceeded";

            internal static string ClassifyRetryReason(Exception ex) => ex switch
            {
                SquirixTransportException rx => ClassifyRetryReason(rx.StatusCode),
                HttpRequestException => "http_request",
                _ => "transient",
            };

            private static string ClassifyRetryReason(CacheTransportStatusCode statusCode) => statusCode switch
            {
                CacheTransportStatusCode.Unavailable => "unavailable",
                CacheTransportStatusCode.Internal => "internal",
                CacheTransportStatusCode.ResourceExhausted => "resource_exhausted",
                _ => "transient",
            };
        }

        private static class OperationCancellationClassifier
        {
            internal static CancellationScenarioKind ClassifyPeerCallAttemptCancellation(
                CancellationToken callerToken,
                CancellationToken operationEffectiveToken,
                CancellationToken perAttemptCompositeToken) => ClassifyFromLinkedTokenState(
                callerToken.IsCancellationRequested,
                operationEffectiveToken.IsCancellationRequested,
                perAttemptCompositeToken.IsCancellationRequested);

            internal static bool EffectiveTokenAllowsRetryAttempt(CancellationToken operationEffectiveToken) => !operationEffectiveToken.IsCancellationRequested;

            private static CancellationScenarioKind ClassifyFromLinkedTokenState(bool callerCanceled, bool operationEffectiveCanceled, bool perAttemptScopeCanceled) =>
                (callerCanceled, operationEffectiveCanceled, perAttemptScopeCanceled) switch
                {
                    (true, _, _) => CancellationScenarioKind.CallerCanceled,
                    (_, true, _) => CancellationScenarioKind.OperationDeadlineExceeded,
                    (_, _, true) => CancellationScenarioKind.PerAttemptTimedOut,
                    _ => CancellationScenarioKind.UnknownCancellation,
                };
        }
    }
}
