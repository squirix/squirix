using System;
using System.Diagnostics;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal.Cluster.Observability;

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
    private bool _disposed;
    private volatile bool _draining;
    private bool _semaphoreDisposed;

    internal CallPolicy(
        TimeSpan? timeoutPerAttempt = null,
        int maxAttempts = 3,
        TimeSpan? baseBackoff = null,
        TimeSpan? maxBackoff = null,
        int maxConcurrentPerPeer = 64,
        string? peer = null,
        TimeProvider? timeProvider = null)
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
        _executor = new CallPolicyExecutor(this, settings, timeProvider ?? TimeProvider.System, _semaphore);
    }

    public void BeginDrain() => _draining = true;

    public ValueTask DisposeAsync()
    {
        lock (_disposeGate)
        {
            if (_disposeTask is not null)
                return new ValueTask(_disposeTask);

            _draining = true;
            _disposed = true;
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

    public async ValueTask<T> ExecuteAsync<TState, T>(Func<TState, CancellationToken, ValueTask<T>> action, TState state, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ThrowIfDraining();

        _activeOperations.Enter();

        try
        {
            cancellationToken.ThrowIfCancellationRequested(); // Ensure we never continue with a canceled token

            var budgetRemaining = RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            if (budgetRemaining is null)
                return await _executor.RunQueuedExecutionAsync(action, state, false, cancellationToken, cancellationToken).ConfigureAwait(false);

            if (cancellationToken.CanBeCanceled)
            {
                using var budgetCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await ConfigureBudgetDeadlineAsync(budgetCts, budgetRemaining.Value).ConfigureAwait(false);
                return await _executor.RunQueuedExecutionAsync(action, state, true, budgetCts.Token, cancellationToken).ConfigureAwait(false);
            }

            using var standaloneBudgetCts = new CancellationTokenSource();
            await ConfigureBudgetDeadlineAsync(standaloneBudgetCts, budgetRemaining.Value).ConfigureAwait(false);
            return await _executor.RunQueuedExecutionAsync(action, state, true, standaloneBudgetCts.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseActiveOperation();
        }
    }

    internal ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken) =>
        ExecuteAsync(static (callback, token) => callback(token), action, cancellationToken);

    private static async ValueTask ConfigureBudgetDeadlineAsync(CancellationTokenSource budgetCts, TimeSpan budgetRemaining)
    {
        if (budgetRemaining <= TimeSpan.Zero)
            await budgetCts.CancelAsync().ConfigureAwait(false);
        else
            budgetCts.CancelAfter(budgetRemaining);
    }

    private void DisposeSemaphoreUnderLockIfIdle()
    {
        if (!_disposed || _semaphoreDisposed || !_activeOperations.IsIdle())
            return;

        _semaphore.Dispose();
        _semaphoreDisposed = true;
        _ = _disposeTcs?.TrySetResult(true);
    }

    private void ReleaseActiveOperation()
    {
        if (!_activeOperations.TryExitToIdle())
            return;

        lock (_disposeGate)
            DisposeSemaphoreUnderLockIfIdle();
    }

    private void ThrowIfDisposed()
    {
        if (!_disposed)
            return;

        throw new ObjectDisposedException(nameof(CallPolicy));
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

        internal bool IsIdle() => Volatile.Read(ref _count) is 0;

        /// <summary>Decrements the counter.</summary>
        /// <returns><see langword="true" /> when the count reaches zero; otherwise <see langword="false" />.</returns>
        internal bool TryExitToIdle() => Interlocked.Decrement(ref _count) is 0;
    }

    private sealed class CallPolicyExecutor
    {
        private readonly TimeSpan _baseBackoff;
        private readonly int _maxAttempts;
        private readonly TimeSpan _maxBackoff;
        private readonly CallPolicy _owner;
        private readonly string _peer;
        private readonly SemaphoreSlim _semaphore;
        private readonly TimeProvider _timeProvider;
        private readonly TimeSpan _timeoutPerAttempt;

        internal CallPolicyExecutor(CallPolicy owner, CallPolicySettings settings, TimeProvider timeProvider, SemaphoreSlim semaphore)
        {
            _owner = owner;
            _peer = settings.Peer;
            _maxAttempts = settings.MaxAttempts;
            _timeoutPerAttempt = settings.TimeoutPerAttempt;
            _baseBackoff = settings.BaseBackoff;
            _maxBackoff = settings.MaxBackoff;
            _timeProvider = timeProvider;
            _semaphore = semaphore;
        }

        internal async ValueTask<T> RunQueuedExecutionAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            bool hasDeadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var queueWaitStarted = Stopwatch.GetTimestamp();
            await _semaphore.WaitAsync(effectiveToken).ConfigureAwait(false);
            CallPolicyMetrics.ObserveQueueWaitSeconds(_peer, Stopwatch.GetElapsedTime(queueWaitStarted));
            try
            {
                ThrowIfDraining();
                return await RunRetryLoopAsync(action, state, hasDeadlineBudget, effectiveToken, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _semaphore.Release();
            }
        }

        private static bool ShouldUseEffectiveTokenDirectly(TimeSpan? budgetRemaining, TimeSpan perAttempt) => budgetRemaining is not null && perAttempt >= budgetRemaining.Value;

        private Task BackoffAsync(TimeSpan span, CancellationToken cancellationToken)
        {
            CallPolicyMetrics.IncrementBackoffLabel(_peer, 1);
            CallPolicyMetrics.ObserveBackoffSeconds(_peer, span);
            return Task.Delay(span, _timeProvider, cancellationToken);
        }

        private async Task<Exception> BackoffOrCaptureCancellationAsync(TimeSpan delay, Exception last, CancellationToken outerCt)
        {
            try
            {
                await BackoffAsync(delay, outerCt).ConfigureAwait(false);
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

        private async ValueTask<AttemptOutcome<T>> ExecuteAttemptCoreAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            int attempt,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken,
            CancellationToken attemptToken)
        {
            try
            {
                return AttemptOutcome<T>.Success(await action(state, attemptToken).ConfigureAwait(false));
            }
            catch (OperationCanceledException oce)
            {
                return await MapOperationCanceledFailureAsync<T>(oce, attempt, effectiveToken, cancellationToken, attemptToken).ConfigureAwait(false);
            }
            catch (RpcException rx)
            {
                return await MapRpcFailureAsync<T>(rx, attempt, effectiveToken).ConfigureAwait(false);
            }
            catch (HttpRequestException ex)
            {
                return await MapHttpFailureAsync<T>(ex, attempt, effectiveToken).ConfigureAwait(false);
            }
        }

        private TimeSpan GetAttemptTimeoutForRemaining(TimeSpan? remaining)
        {
            if (remaining is null)
                return _timeoutPerAttempt;

            if (remaining <= TimeSpan.Zero)
                return TimeSpan.Zero;

            return remaining.Value < _timeoutPerAttempt ? remaining.Value : _timeoutPerAttempt;
        }

        private async ValueTask<AttemptOutcome<T>> MapHttpFailureAsync<T>(HttpRequestException ex, int attempt, CancellationToken effectiveToken)
        {
            if (attempt >= _maxAttempts || !OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                return AttemptOutcome<T>.Stop(ex);

            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(ex));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), ex, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapOperationCanceledFailureAsync<T>(
            OperationCanceledException oce,
            int attempt,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken,
            CancellationToken attemptToken)
        {
            var cancelKind = OperationCancellationClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptToken);
            if (cancelKind is not CancellationScenarioKind.PerAttemptTimedOut || attempt >= _maxAttempts)
                return AttemptOutcome<T>.Stop(oce);

            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "operation_canceled").Inc();
            CallPolicyMetrics.IncrementRetriesTotal(_peer, "operation_canceled");
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), oce, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<AttemptOutcome<T>> MapRpcFailureAsync<T>(RpcException rx, int attempt, CancellationToken effectiveToken)
        {
            var canRetry = attempt < _maxAttempts && OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken);
            if (!canRetry)
                return AttemptOutcome<T>.Stop(rx);

            if (rx.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded)
            {
                var reason = rx.StatusCode is StatusCode.DeadlineExceeded ? CallPolicyRetryClassifier.DeadlineExceeded : CallPolicyRetryClassifier.Canceled;
                RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", reason).Inc();
                CallPolicyMetrics.IncrementRetriesTotal(_peer, reason);
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, effectiveToken).ConfigureAwait(false));
            }

            if (rx.StatusCode is not (StatusCode.Unavailable or StatusCode.Internal or StatusCode.ResourceExhausted))
                return AttemptOutcome<T>.Stop(rx);
            CallPolicyMetrics.IncrementRetriesTotal(_peer, CallPolicyRetryClassifier.ClassifyRetryReason(rx));
            return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, effectiveToken).ConfigureAwait(false));
        }

        private async ValueTask<T> RunRetryLoopAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            bool hasDeadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var attempt = 0;
            Exception? last = null;

            while (OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts)
            {
                attempt++;
                var outcome = await TryOneAttemptAsync(action, state, attempt, effectiveToken, cancellationToken).ConfigureAwait(false);
                if (outcome.Succeeded)
                    return outcome.Value!;

                last = outcome.LastException;
                if (!outcome.ShouldRetry)
                    break;
            }

            cancellationToken.ThrowIfCancellationRequested();
            ThrowAfterFailedAttempts(last, hasDeadlineBudget, effectiveToken);
            throw new InvalidOperationException("Unreachable retry loop exit.");
        }

        private void ThrowAfterFailedAttempts(Exception? last, bool hasDeadlineBudget, CancellationToken effectiveToken)
        {
            if (!hasDeadlineBudget || OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                throw last switch
                {
                    TaskCanceledException or OperationCanceledException => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts timed out.")),
                    RpcException { StatusCode: StatusCode.Cancelled } => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout.")),
                    _ => last!,
                };

            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private void ThrowIfDraining()
        {
            if (!_owner._draining)
                return;

            CallPolicyMetrics.IncrementDrainRejectsTotal(_peer, 1);
            throw new RpcException(new Status(StatusCode.Unavailable, "Peer client pool is draining."));
        }

        private async ValueTask<AttemptOutcome<T>> TryOneAttemptAsync<TState, T>(
            Func<TState, CancellationToken, ValueTask<T>> action,
            TState state,
            int attempt,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var budgetRemaining = RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            var perAttempt = GetAttemptTimeoutForRemaining(budgetRemaining);
            if (ShouldUseEffectiveTokenDirectly(budgetRemaining, perAttempt))
                return await ExecuteAttemptCoreAsync(action, state, attempt, effectiveToken, cancellationToken, effectiveToken).ConfigureAwait(false);

            if (effectiveToken.CanBeCanceled)
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveToken);
                if (budgetRemaining is null || perAttempt < budgetRemaining.Value)
                    attemptCts.CancelAfter(perAttempt);

                return await ExecuteAttemptCoreAsync(action, state, attempt, effectiveToken, cancellationToken, attemptCts.Token).ConfigureAwait(false);
            }

            using var standaloneAttemptCts = new CancellationTokenSource();
            if (budgetRemaining is null || perAttempt < budgetRemaining.Value)
                standaloneAttemptCts.CancelAfter(perAttempt);

            return await ExecuteAttemptCoreAsync(action, state, attempt, effectiveToken, cancellationToken, standaloneAttemptCts.Token).ConfigureAwait(false);
        }

        private sealed record AttemptOutcome<T>
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

            internal static bool OperationEffectiveTokenAllowsRetryAttempt(CancellationToken operationEffectiveToken) => !operationEffectiveToken.IsCancellationRequested;

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
