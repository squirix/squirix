using System;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Threading;
using Squirix.Transport.Grpc;

namespace Squirix.Server.Cluster;

internal sealed class ServerCallPolicy : IServerCallPolicy
{
    private readonly ServerActiveOperationCounter _activeOperations = new();
    private readonly Lock _disposeGate = new();
    private readonly VolatileBool _draining = new();
    private readonly ServerCallPolicyExecutor _executor;
    private readonly ServerCallPolicyMetrics _metrics;
    private readonly string _peer;
    private readonly SemaphoreSlim _semaphore;
    private Task? _disposeTask;
    private TaskCompletionSource<bool>? _disposeTcs;
    private int _disposed;
    private bool _semaphoreDisposed;

    internal ServerCallPolicy(
        ServerCallPolicyInstrumentation instrumentation,
        TimeProvider timeProvider,
        int maxAttempts = 3,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        CallPolicyTimeouts? timeouts = null)
    {
        var metrics = instrumentation.CallPolicyMetrics;
        var rpcMetrics = instrumentation.RpcTimeoutMetrics;
        _metrics = metrics;
        _peer = string.IsNullOrWhiteSpace(peer) ? "unknown" : peer;
        var cap = Math.Max(1, maxConcurrentPerPeer);
        _semaphore = new SemaphoreSlim(cap, cap);
        var settings = new ServerCallPolicySettings(
            _peer,
            Math.Max(1, maxAttempts),
            timeouts?.TimeoutPerAttempt ?? TimeSpan.FromMilliseconds(600),
            timeouts?.BaseBackoff ?? TimeSpan.FromMilliseconds(50),
            timeouts?.MaxBackoff ?? TimeSpan.FromMilliseconds(500));
        _executor = new ServerCallPolicyExecutor(settings, timeProvider, _semaphore, IsDraining, metrics, rpcMetrics);
    }

    public void BeginDrain() => _draining.Write(true);

    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
        {
            if (_disposeTask != null)
                return new ValueTask(_disposeTask);

            _draining.Write(true);
            _ = Interlocked.Exchange(ref _disposed, 1);
            if (_activeOperations.CheckIfIdle())
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

    public async ValueTask<T> ExecuteAsync<TState, T>(TState state, Func<TState, CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken)
    {
        _activeOperations.Enter();
        try
        {
            // Disposal outranks draining so callers observe the same failure mode as before the
            // claim-then-recheck reorder; the post-enter recheck is what closes the race with a concurrent dispose.
            ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
            ThrowIfDraining();
            cancellationToken.ThrowIfCancellationRequested();

            var deadlineBudget = ServerRpcDeadlineContext.CurrentBudget;
            if (deadlineBudget is not { } budget || _executor.UsesClock(budget.Clock))
                return await _executor.RunExecutionAsync(state, action, deadlineBudget, cancellationToken, cancellationToken).ConfigureAwait(false);

            // Independent clocks require an operation-wide budget token alongside the attempt timer.
            var budgetRemaining = budget.Remaining;
            var budgetDelay = budgetRemaining > TimeSpan.Zero ? budgetRemaining : TimeSpan.Zero;
            using var budgetScope = new TimedCancellation(budgetDelay, budget.Clock, cancellationToken);
            return await _executor.RunExecutionAsync(state, action, deadlineBudget, budgetScope.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseActiveOperation();
        }
    }

    private void DisposeSemaphoreUnderLockIfIdle()
    {
        if (Volatile.Read(ref _disposed) == 0 || _semaphoreDisposed || !_activeOperations.CheckIfIdle())
            return;

        _semaphore.Dispose();
        _semaphoreDisposed = true;
        _ = _disposeTcs?.TrySetResult(true);
    }

    private bool IsDraining() => _draining.Read();

    private void ReleaseActiveOperation()
    {
        if (!_activeOperations.TryExitToIdle())
            return;

        lock (_disposeGate)
            DisposeSemaphoreUnderLockIfIdle();
    }

    private void ThrowIfDraining()
    {
        if (!_draining.Read())
            return;

        _metrics.IncrementDrainRejectsTotal(_peer, 1);
        throw new RpcException(new Status(StatusCode.Unavailable, "ServerPeer client pool is draining."));
    }

    [Immutable]
    private sealed record ServerCallPolicySettings(string Peer, int MaxAttempts, TimeSpan TimeoutPerAttempt, TimeSpan BaseBackoff, TimeSpan MaxBackoff);

    /// <summary>Thread-safe in-flight operation counter for call-policy dispose gating.</summary>
    private sealed class ServerActiveOperationCounter
    {
        private int _count;

        internal bool CheckIfIdle() => Volatile.Read(ref _count) == 0;

        internal void Enter() => _ = Interlocked.Increment(ref _count);

        /// <summary>Decrements the counter.</summary>
        /// <returns><see langword="true" /> when the count reaches zero; otherwise <see langword="false" />.</returns>
        internal bool TryExitToIdle() => Interlocked.Decrement(ref _count) == 0;
    }

    [Immutable]
    private sealed class ServerCallPolicyExecutor
    {
        private readonly TimeSpan _baseBackoff;
        private readonly Func<bool> _isDraining;
        private readonly int _maxAttempts;
        private readonly TimeSpan _maxBackoff;
        private readonly ServerCallPolicyMetrics _metrics;
        private readonly string _peer;
        private readonly ServerRpcTimeoutMetrics _rpcMetrics;
        private readonly SemaphoreSlim _semaphore;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _timeoutPerAttempt;

        internal ServerCallPolicyExecutor(
            ServerCallPolicySettings settings,
            TimeProvider timeProvider,
            SemaphoreSlim semaphore,
            Func<bool> isDraining,
            ServerCallPolicyMetrics metrics,
            ServerRpcTimeoutMetrics rpcMetrics)
        {
            _peer = settings.Peer;
            _maxAttempts = settings.MaxAttempts;
            _timeoutPerAttempt = settings.TimeoutPerAttempt;
            _baseBackoff = settings.BaseBackoff;
            _maxBackoff = settings.MaxBackoff;
            _timeProvider = timeProvider;
            _semaphore = semaphore;
            _isDraining = isDraining;
            _metrics = metrics;
            _rpcMetrics = rpcMetrics;
        }

        internal bool UsesClock(TimeProvider clock) => ReferenceEquals(_timeProvider, clock);

        internal async ValueTask<T> RunExecutionAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            ServerDeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            // The permit is non-queuing: a caller that cannot take a free slot at once is refused instead of
            // holding its entry admission slot while it waits on a peer that may be hung. A drain in progress
            // outranks the busy refusal so callers see the shutdown reason.
            if (!await _semaphore.WaitAsync(TimeSpan.Zero, CancellationToken.None).ConfigureAwait(false))
            {
                ThrowIfDraining();
                _metrics.IncrementBusyRejectsTotal(_peer, 1);
                throw ServerOpContract.TooManyRequests("peer_busy");
            }

            try
            {
                ThrowIfDraining();
                return await RunRetryLoopAsync(state, action, budget, effectiveToken, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _semaphore.Release();
            }
        }

        private static bool ShouldUseEffectiveTokenDirectly(TimeSpan? budgetRemaining, TimeSpan perAttempt) => budgetRemaining != null && perAttempt >= budgetRemaining.Value;

        private RpcException CreateBudgetExceeded()
        {
            _rpcMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            return new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private bool IsSharedBudgetSpent(ServerDeadlineBudget? budget) => budget is { } value && UsesClock(value.Clock) && value.Remaining <= TimeSpan.Zero;

        private Task BackoffAsync(TimeSpan d, CancellationToken outerCt)
        {
            _metrics.IncrementBackoffLabel(_peer, 1);
            _metrics.ObserveBackoffSeconds(_peer, d);
            return Task.Delay(d, _timeProvider, outerCt);
        }

        private async Task<Exception> BackoffOrCaptureCancellationAsync(TimeSpan delay, Exception last, ServerDeadlineBudget? budget, CancellationToken outerCt)
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

        private TimeSpan GetAttemptTimeoutForRemaining(TimeSpan? remaining) => remaining switch
        {
            null => _timeoutPerAttempt,
            { } r when r <= TimeSpan.Zero => TimeSpan.Zero,
            { } r when _timeoutPerAttempt == Timeout.InfiniteTimeSpan => r,
            { } r when r < _timeoutPerAttempt => r,
            _ => _timeoutPerAttempt,
        };

        private async ValueTask<AttemptOutcome<T>> MapHttpFailureAsync<T>(HttpRequestException ex, int attempt, ServerDeadlineBudget? budget, CancellationToken effectiveToken)
        {
            if (attempt >= _maxAttempts || !ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) || IsSharedBudgetSpent(budget))
                return AttemptOutcome<T>.Stop(ex);

            _metrics.IncrementRetriesTotal(_peer, ServerCallPolicyRetryClassifier.ClassifyRetryReason(ex));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), ex, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapOperationCanceledFailureAsync<T>(
            OperationCanceledException oce,
            int attempt,
            ServerDeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken,
            CancellationToken attemptToken)
        {
            if (!cancellationToken.IsCancellationRequested && IsSharedBudgetSpent(budget))
                return AttemptOutcome<T>.Stop(oce);

            var cancelKind = ServerCancelClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptToken);
            if (cancelKind != ServerCancelScenarioKind.PerAttemptTimedOut || attempt >= _maxAttempts)
                return AttemptOutcome<T>.Stop(oce);

            _rpcMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "operation_canceled").Inc();
            _metrics.IncrementRetriesTotal(_peer, "operation_canceled");
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), oce, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapRpcFailureAsync<T>(RpcException rx, int attempt, ServerDeadlineBudget? budget, CancellationToken effectiveToken)
        {
            // A mutation may have committed durably even though its outcome is unknown (write-ahead idempotency
            // intent). Re-sending the same operation would re-execute it, so the caller must stop retrying and
            // let the replication/result layer resolve the ambiguous commit by journal index.
            if (ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(rx.Status.Detail))
                return AttemptOutcome<T>.Stop(rx);

            // A ring refusal comes from a configuration disagreement that a retry cannot fix; nothing was executed on the peer.
            if (RingMismatchFailure.IsRefusal(rx))
                return AttemptOutcome<T>.Stop(rx);

            var canRetry = attempt < _maxAttempts && ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget);
            if (!canRetry)
                return AttemptOutcome<T>.Stop(rx);

            if (rx.StatusCode == StatusCode.Cancelled || rx.StatusCode == StatusCode.DeadlineExceeded)
            {
                var reason = rx.StatusCode == StatusCode.DeadlineExceeded ? ServerCallPolicyRetryClassifier.DeadlineExceeded : ServerCallPolicyRetryClassifier.Canceled;
                _rpcMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", reason).Inc();
                _metrics.IncrementRetriesTotal(_peer, reason);
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
            }

            // ResourceExhausted is an admission refusal by the peer, not a transient fault: it passes through instead of being retried against a peer that just refused.
            if (rx.StatusCode is not (StatusCode.Unavailable or StatusCode.Internal))
                return AttemptOutcome<T>.Stop(rx);
            _metrics.IncrementRetriesTotal(_peer, ServerCallPolicyRetryClassifier.ClassifyRetryReason(rx));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, budget, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<T> RunRetryLoopAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            ServerDeadlineBudget? budget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var attempt = 0;
            Exception? last = null;

            while (ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts && !IsSharedBudgetSpent(budget))
            {
                attempt++;
                var outcome = await OneAttemptAsync(state, action, attempt, budget, effectiveToken, cancellationToken).ConfigureAwait(false);
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

        private void ThrowAfterFailedAttempts(Exception? last, ServerDeadlineBudget? budget, CancellationToken effectiveToken)
        {
            // Terminal mutation and topology refusals keep their no-retry contract at the deadline boundary.
            if (last is RpcException rpc && (ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(rpc.Status.Detail) || RingMismatchFailure.IsRefusal(rpc)))
                throw rpc;

            if (budget == null || (ServerCancelClassifier.EffectiveTokenAllowsRetryAttempt(effectiveToken) && !IsSharedBudgetSpent(budget)))
            {
                throw last switch
                {
                    TaskCanceledException or OperationCanceledException => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts timed out.")),
                    RpcException { StatusCode: StatusCode.Cancelled } => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout.")),
                    _ => last!,
                };
            }

            _rpcMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private void ThrowIfDraining()
        {
            if (!_isDraining())
                return;

            _metrics.IncrementDrainRejectsTotal(_peer, 1);
            throw new RpcException(new Status(StatusCode.Unavailable, "ServerPeer client pool is draining."));
        }

        private async ValueTask<AttemptOutcome<T>> OneAttemptAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            int attempt,
            ServerDeadlineBudget? deadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var budget = deadlineBudget?.Remaining;
            var remaining = GetAttemptTimeoutForRemaining(budget);

            // Shared-clock calls use one attempt timer for both limits; independent clocks retain their operation-wide token.
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
                RpcException rpc when ServerOpContractClassifier.IsCommitOutcomeUnknownDetail(rpc.Status.Detail) || RingMismatchFailure.IsRefusal(rpc) => AttemptOutcome<T>.Stop(rpc),
                _ when attemptBoundsBudget && attemptToken.IsCancellationRequested && !cancellationToken.IsCancellationRequested => throw CreateBudgetExceeded(),
                OperationCanceledException oce => await MapOperationCanceledFailureAsync<T>(oce, attempt, deadlineBudget, effectiveToken, cancellationToken, attemptToken).ConfigureAwait(false),
                RpcException rx => await MapRpcFailureAsync<T>(rx, attempt, deadlineBudget, effectiveToken).ConfigureAwait(false),
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

        private static class ServerCallPolicyRetryClassifier
        {
            internal const string Canceled = "canceled";
            internal const string DeadlineExceeded = "deadline_exceeded";

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
    }
}
