using System;
using System.Diagnostics;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Observability;
using Squirix.Transport.Grpc;

namespace Squirix.Internal.Cluster.Reliability;

internal sealed class CallPolicy : ICallPolicy
{
    private readonly ActiveOperationCounter _activeOperations = new();
    private readonly Lock _disposeGate = new();
    private readonly CallPolicyExecutor _executor;
    private readonly string _peer;
    private readonly SemaphoreSlim _semaphore;
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
        int maxConcurrentPerPeer = 64,
        string? peer = null)
    {
        _peer = string.IsNullOrWhiteSpace(peer) ? "unknown" : peer;
        var cap = Math.Max(1, maxConcurrentPerPeer);
        _semaphore = new SemaphoreSlim(cap, cap);
        var settings = new CallPolicySettings(
            _peer,
            Math.Max(1, maxAttempts),
            timeoutPerAttempt ?? TimeSpan.FromMilliseconds(600),
            baseBackoff ?? TimeSpan.FromMilliseconds(50),
            maxBackoff ?? TimeSpan.FromMilliseconds(500));
        _executor = new CallPolicyExecutor(settings, timeProvider, _semaphore, IsDraining);
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

    [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
    public async ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken, DeadlineBudget? budget = null)
    {
        _activeOperations.Enter();

        try
        {
            // Disposal outranks draining so callers observe the same failure mode as before the
            // claim-then-recheck reorder; the post-enter recheck is what closes the race with a concurrent dispose.
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            ThrowIfDraining();
            cancellationToken.ThrowIfCancellationRequested(); // Ensure we never continue with a canceled token

            if (budget is not { } deadlineBudget || _executor.UsesClock(deadlineBudget.Clock))
                return await _executor.RunQueuedExecutionAsync(action, state, budget, cancellationToken, cancellationToken).ConfigureAwait(false);

            // Independent clocks require an operation-wide budget token alongside the attempt timer.
            var budgetRemaining = deadlineBudget.Remaining;
            var budgetDelay = budgetRemaining > TimeSpan.Zero ? budgetRemaining : TimeSpan.Zero;
            using var budgetScope = new TimedCancellation(budgetDelay, deadlineBudget.Clock, cancellationToken);
            return await _executor.RunQueuedExecutionAsync(action, state, budget, budgetScope.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseActiveOperation();
        }
    }

    private void DisposeSemaphoreUnderLockIfIdle()
    {
        if (Volatile.Read(ref _disposed) == 0 || _semaphoreDisposed || !_activeOperations.IsIdle())
            return;

        _semaphore.Dispose();
        _semaphoreDisposed = true;
        _ = _disposeTcs?.TrySetResult(true);
    }

    private bool IsDraining() => _draining;

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
        throw new RpcException(new Status(StatusCode.Unavailable, "Peer client pool is draining."));
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
        private readonly Func<bool> _isDraining;
        private readonly int _maxAttempts;
        private readonly TimeSpan _maxBackoff;
        private readonly string _peer;
        private readonly SemaphoreSlim _semaphore;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _timeoutPerAttempt;

        internal CallPolicyExecutor(CallPolicySettings settings, TimeProvider timeProvider, SemaphoreSlim semaphore, Func<bool> isDraining)
        {
            _peer = settings.Peer;
            _maxAttempts = settings.MaxAttempts;
            _timeoutPerAttempt = settings.TimeoutPerAttempt;
            _baseBackoff = settings.BaseBackoff;
            _maxBackoff = settings.MaxBackoff;
            _timeProvider = timeProvider;
            _semaphore = semaphore;
            _isDraining = isDraining;
        }

        internal bool UsesClock(TimeProvider clock) => ReferenceEquals(_timeProvider, clock);

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        internal async ValueTask<T> RunQueuedExecutionAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            DeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var queueWaitStarted = Stopwatch.GetTimestamp();
            var sharedClock = budget is { } value && UsesClock(value.Clock);
            var acquired = sharedClock && await _semaphore.WaitAsync(0, effectiveToken).ConfigureAwait(false);
            using var queuedScope = CreateQueuedScope(acquired, budget, effectiveToken);
            if (queuedScope != null)
                effectiveToken = queuedScope.Token;

            try
            {
                if (!acquired)
                    await _semaphore.WaitAsync(effectiveToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (budget != null && !cancellationToken.IsCancellationRequested)
            {
                // The operation deadline can expire while queued on the per-peer semaphore. Surface it as
                // the same deadline-budget RpcException the retry loop produces instead of leaking a raw
                // OperationCanceledException.
                RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
            }

            CallPolicyMetrics.ObserveQueueWaitSeconds(_peer, Stopwatch.GetElapsedTime(queueWaitStarted));
            try
            {
                ThrowIfDraining();
                var attempt = 0;
                Exception? last = null;

                while (OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts && !IsSharedBudgetSpent(budget))
                {
                    attempt++;
                    var outcome = await OneAttemptAsync(action, state, attempt, budget, effectiveToken, cancellationToken).ConfigureAwait(false);
                    if (outcome.Succeeded)
                        return outcome.Value!;

                    last = outcome.LastException;
                    if (!outcome.ShouldRetry)
                        break;
                }

                cancellationToken.ThrowIfCancellationRequested();
                ThrowAfterFailedAttempts(last, budget, effectiveToken);
                throw new InvalidOperationException("Unreachable retry loop exit.");
            }
            finally
            {
                _ = _semaphore.Release();
            }
        }

        private static bool ShouldUseEffectiveTokenDirectly(TimeSpan? budgetRemaining, TimeSpan perAttempt) => budgetRemaining != null && perAttempt >= budgetRemaining.Value;

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
                if (exhaustsBudget)
                {
                    outerCt.ThrowIfCancellationRequested();
                    throw CreateBudgetExceeded();
                }

                return last;
            }
            catch (OperationCanceledException oce) when (outerCt.IsCancellationRequested)
            {
                return oce;
            }
        }

        private TimeSpan BackoffWithJitter(int attempt)
        {
            // Exponential backoff with capped growth
            var pow = Math.Min(attempt - 1, 6);
            var cappedMs = Math.Min(_maxBackoff.TotalMilliseconds, _baseBackoff.TotalMilliseconds * Math.Pow(2, pow));

            // Use jitter factor in [0.5, 1.0) to avoid near-zero waits
            var jitterFactor = 0.5 + (RandomNumberGenerator.GetInt32(0, 5000) / 10000.0);
            var candidateMs = cappedMs * jitterFactor;

            // Enforce a small floor (50ms) per backoff when cap permits, to avoid flaky sub-50ms totals
            var floorMs = Math.Min(50.0, cappedMs);
            var finalMs = Math.Max(candidateMs, floorMs);

            return TimeSpan.FromMilliseconds(finalMs);
        }

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

        private RpcException CreateBudgetExceeded()
        {
            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            return new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private async ValueTask<AttemptOutcome<T>> MapHttpFailureAsync<T>(HttpRequestException ex, int attempt, DeadlineBudget? budget, CancellationToken effectiveToken)
        {
            if (attempt >= _maxAttempts || !OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) || IsSharedBudgetSpent(budget))
                return AttemptOutcome<T>.Stop(ex);

            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(ex));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), ex, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapOperationCanceledFailureAsync<T>(
            OperationCanceledException oce,
            int attempt,
            DeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken,
            CancellationToken attemptToken)
        {
            if (!cancellationToken.IsCancellationRequested && IsSharedBudgetSpent(budget))
                return AttemptOutcome<T>.Stop(oce);

            var cancelKind = OperationCancellationClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptToken);
            if (cancelKind != CancellationScenarioKind.PerAttemptTimedOut || attempt >= _maxAttempts)
                return AttemptOutcome<T>.Stop(oce);

            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "operation_canceled").Inc();
            CallPolicyMetrics.IncrementRetriesTotal(_peer, "operation_canceled");
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), oce, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapRpcFailureAsync<T>(RpcException rx, int attempt, DeadlineBudget? budget, CancellationToken effectiveToken, CancellationToken cancellationToken)
        {
            // A mutation may have committed durably even though its outcome is unknown (server write-ahead
            // idempotency intent). Re-sending the same operation would not re-execute (the server gates on the
            // intent), but the outcome is still ambiguous: stop retrying and surface it to the caller.
            if (CallPolicyRetryClassifier.IsCommitOutcomeUnknownStatus(rx))
                return AttemptOutcome<T>.Stop(rx);

            // The gRPC deadline timer and the budget timer expire at the same instant: when gRPC reports the
            // expiry first, it is the exhausted operation budget, not a per-attempt timeout to retry.
            if (rx.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded && !cancellationToken.IsCancellationRequested &&
                budget?.Remaining is { } remainingBudget && remainingBudget <= TimeSpan.Zero)
            {
                RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
            }

            var canRetry = attempt < _maxAttempts && OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget);
            if (!canRetry)
                return AttemptOutcome<T>.Stop(rx);

            if (rx.StatusCode == StatusCode.Cancelled || rx.StatusCode == StatusCode.DeadlineExceeded)
            {
                var reason = rx.StatusCode == StatusCode.DeadlineExceeded ? CallPolicyRetryClassifier.DeadlineExceeded : CallPolicyRetryClassifier.Canceled;
                RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", reason).Inc();
                CallPolicyMetrics.IncrementRetriesTotal(_peer, reason);
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
            }

            if (rx.StatusCode is not (StatusCode.Unavailable or StatusCode.Internal or StatusCode.ResourceExhausted))
                return AttemptOutcome<T>.Stop(rx);
            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(rx));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
        }

        private void ThrowAfterFailedAttempts(Exception? last, DeadlineBudget? budget, CancellationToken effectiveToken)
        {
            // An ambiguous durable outcome must retain its no-retry contract even at the deadline boundary.
            if (last is RpcException rpc && CallPolicyRetryClassifier.IsCommitOutcomeUnknownStatus(rpc))
                throw rpc;

            if (budget == null || (OperationCancellationClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget)))
            {
                throw last switch
                {
                    TaskCanceledException or OperationCanceledException => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts timed out.")),
                    RpcException { StatusCode: StatusCode.Cancelled } => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout.")),
                    _ => last!,
                };
            }

            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private void ThrowIfDraining()
        {
            if (!_isDraining())
                return;

            CallPolicyMetrics.IncrementDrainRejectsTotal(_peer, 1);
            throw new RpcException(new Status(StatusCode.Unavailable, "Peer client pool is draining."));
        }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        private async ValueTask<AttemptOutcome<T>> OneAttemptAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            int attempt,
            DeadlineBudget? deadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var budget = deadlineBudget?.Remaining;
            var remaining = GetAttemptTimeoutForRemaining(budget);

            // Queued calls retain an operation-wide token; uncontended shared-clock calls use one timer for both limits.
            var sharedClockBudget = deadlineBudget is { } value && UsesClock(value.Clock) && effectiveToken == cancellationToken;
            var attemptScope = !sharedClockBudget && ShouldUseEffectiveTokenDirectly(budget, remaining) ? null : new TimedCancellation(remaining, _timeProvider, effectiveToken);
            var attemptToken = attemptScope?.Token ?? effectiveToken;
            var attemptBoundsBudget = sharedClockBudget && budget is { } budgetRemaining && remaining >= budgetRemaining;
            Exception failure;
            try
            {
                return AttemptOutcome<T>.Success(await action(state, attemptToken).ConfigureAwait(false));
            }
            catch (Exception ex) when (ex is OperationCanceledException or RpcException or HttpRequestException)
            {
                failure = ex;
            }
            finally
            {
                // Attempt cancellation is no longer needed while failure mapping waits for retry backoff.
                attemptScope?.Dispose();
            }

            // The timer records which limit won even if its callback precedes the next timestamp observation.
            return failure switch
            {
                RpcException rpc when CallPolicyRetryClassifier.IsCommitOutcomeUnknownStatus(rpc) => AttemptOutcome<T>.Stop(rpc),
                _ when attemptBoundsBudget && attemptToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested => throw CreateBudgetExceeded(),
                OperationCanceledException oce => await MapOperationCanceledFailureAsync<T>(oce, attempt, deadlineBudget, effectiveToken, cancellationToken, attemptToken).ConfigureAwait(false),
                RpcException rx => await MapRpcFailureAsync<T>(rx, attempt, deadlineBudget, effectiveToken, cancellationToken).ConfigureAwait(false),
                HttpRequestException http => await MapHttpFailureAsync<T>(http, attempt, deadlineBudget, effectiveToken).ConfigureAwait(false),
                _ => throw new InvalidOperationException("Unreachable attempt failure classification."),
            };
        }

        [Immutable]
        private readonly record struct AttemptOutcome<T>
        {
            internal Exception? LastException { get; private init; }

            internal bool ShouldRetry { get; private init; }

            internal bool Succeeded { get; private init; }

            internal T? Value { get; private init; }

            internal static AttemptOutcome<T> Retry(Exception last) => new() { ShouldRetry = true, LastException = last };

            internal static AttemptOutcome<T> Stop(Exception last) => new() { ShouldRetry = false, LastException = last };

            internal static AttemptOutcome<T> Success(T value) => new() { Succeeded = true, Value = value };
        }

        private static class CallPolicyRetryClassifier
        {
            internal const string Canceled = "canceled";
            internal const string DeadlineExceeded = "deadline_exceeded";

            private const string CommitOutcomeUnknownDetail = "COMMIT_OUTCOME_UNKNOWN";

            internal static bool IsCommitOutcomeUnknownStatus(RpcException ex) => string.Equals(ex.Status.Detail, CommitOutcomeUnknownDetail, StringComparison.Ordinal);

            internal static string ClassifyRetryReason(Exception ex) => ex switch
            {
                RpcException rx => ClassifyRetryReason(rx.StatusCode),
                HttpRequestException => "http_request",
                TaskCanceledException => "task_canceled",
                OperationCanceledException => "operation_canceled",
                _ => "transient",
            };

            private static string ClassifyRetryReason(StatusCode statusCode) => statusCode switch
            {
                StatusCode.Cancelled => Canceled,
                StatusCode.DeadlineExceeded => DeadlineExceeded,
                StatusCode.Unavailable => "unavailable",
                StatusCode.Internal => "internal",
                StatusCode.ResourceExhausted => "resource_exhausted",
                StatusCode.Aborted => "aborted",
                StatusCode.AlreadyExists => "already_exists",
                StatusCode.NotFound => "not_found",
                StatusCode.PermissionDenied => "permission_denied",
                StatusCode.Unauthenticated => "unauthenticated",
                StatusCode.OutOfRange => "out_of_range",
                StatusCode.Unimplemented => "unimplemented",
                StatusCode.DataLoss => "data_loss",
                StatusCode.FailedPrecondition => "failed_precondition",
                StatusCode.InvalidArgument => "invalid_argument",
                StatusCode.Unknown => "unknown",
                StatusCode.OK => "ok",
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
