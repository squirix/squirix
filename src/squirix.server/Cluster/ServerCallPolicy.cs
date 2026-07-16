using System;
using System.Diagnostics;
using System.Net.Http;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster;

internal sealed class ServerCallPolicy : IServerCallPolicy
{
    private readonly ServerActiveOperationCounter _activeOperations = new();
    private readonly Lock _disposeGate = new();
    private readonly ServerCallPolicyExecutor _executor;
    private readonly string _peer;
    private readonly SemaphoreSlim _semaphore;
    private bool _disposed;
    private Task? _disposeTask;
    private TaskCompletionSource<bool>? _disposeTcs;
    private volatile bool _draining;
    private bool _semaphoreDisposed;

    internal ServerCallPolicy(
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
        _executor = new ServerCallPolicyExecutor(
            new ServerCallPolicySettings(
                _peer,
                Math.Max(1, maxAttempts),
                timeoutPerAttempt ?? TimeSpan.FromMilliseconds(600),
                baseBackoff ?? TimeSpan.FromMilliseconds(50),
                maxBackoff ?? TimeSpan.FromMilliseconds(500)),
            timeProvider ?? TimeProvider.System,
            _semaphore,
            () => _draining);
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
        ThrowIfDisposed();
        ThrowIfDraining();

        _activeOperations.Enter();

        try
        {
            cancellationToken.ThrowIfCancellationRequested(); // Ensure we never continue with a canceled token

            var budgetRemaining = ServerRpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            if (budgetRemaining is null)
                return await _executor.RunQueuedExecutionAsync(state, action, false, cancellationToken, cancellationToken).ConfigureAwait(false);

            if (cancellationToken.CanBeCanceled)
            {
                using var budgetCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                await ConfigureBudgetDeadlineAsync(budgetCts, budgetRemaining.Value).ConfigureAwait(false);
                return await _executor.RunQueuedExecutionAsync(state, action, true, budgetCts.Token, cancellationToken).ConfigureAwait(false);
            }

            using var standaloneBudgetCts = new CancellationTokenSource();
            await ConfigureBudgetDeadlineAsync(standaloneBudgetCts, budgetRemaining.Value).ConfigureAwait(false);
            return await _executor.RunQueuedExecutionAsync(state, action, true, standaloneBudgetCts.Token, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            ReleaseActiveOperation();
        }
    }

    private static async ValueTask ConfigureBudgetDeadlineAsync(CancellationTokenSource budgetCts, TimeSpan budgetRemaining)
    {
        if (budgetRemaining <= TimeSpan.Zero)
            await budgetCts.CancelAsync().ConfigureAwait(false);
        else
            budgetCts.CancelAfter(budgetRemaining);
    }

    private void DisposeSemaphoreUnderLockIfIdle()
    {
        if (!_disposed || _semaphoreDisposed || !_activeOperations.CheckIfIdle())
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

        throw new ObjectDisposedException(nameof(ServerCallPolicy));
    }

    private void ThrowIfDraining()
    {
        if (!_draining)
            return;

        ServerCallPolicyMetrics.IncrementDrainRejectsTotal(_peer, 1);
        throw new RpcException(new Status(StatusCode.Unavailable, "ServerPeer client pool is draining."));
    }

    private sealed record ServerCallPolicySettings(string Peer, int MaxAttempts, TimeSpan TimeoutPerAttempt, TimeSpan BaseBackoff, TimeSpan MaxBackoff);

    /// <summary>Thread-safe in-flight operation counter for call-policy dispose gating.</summary>
    private sealed class ServerActiveOperationCounter
    {
        private int _count;

        internal bool CheckIfIdle() => Volatile.Read(ref _count) is 0;

        internal void Enter() => _ = Interlocked.Increment(ref _count);

        /// <summary>Decrements the counter.</summary>
        /// <returns><see langword="true" /> when the count reaches zero; otherwise <see langword="false" />.</returns>
        internal bool TryExitToIdle() => Interlocked.Decrement(ref _count) is 0;
    }

    private sealed class ServerCallPolicyExecutor
    {
        private readonly TimeSpan _baseBackoff;
        private readonly Func<bool> _isDraining;
        private readonly int _maxAttempts;
        private readonly TimeSpan _maxBackoff;
        private readonly string _peer;
        private readonly SemaphoreSlim _semaphore;
        private readonly TimeSpan _timeoutPerAttempt;
        private readonly TimeProvider _timeProvider;

        internal ServerCallPolicyExecutor(ServerCallPolicySettings settings, TimeProvider timeProvider, SemaphoreSlim semaphore, Func<bool> isDraining)
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

        internal async ValueTask<T> RunQueuedExecutionAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            bool hasDeadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var queueWaitStarted = Stopwatch.GetTimestamp();
            try
            {
                await _semaphore.WaitAsync(effectiveToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (hasDeadlineBudget && !cancellationToken.IsCancellationRequested)
            {
                // The ambient RPC deadline can expire while queued on the per-peer semaphore. Surface it as
                // the same deadline-budget RpcException the retry loop produces instead of leaking a raw
                // TaskCanceledException.
                ServerRpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
            }

            ServerCallPolicyMetrics.ObserveQueueWaitSeconds(_peer, Stopwatch.GetElapsedTime(queueWaitStarted));
            try
            {
                ThrowIfDraining();
                return await RunRetryLoopAsync(state, action, hasDeadlineBudget, effectiveToken, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                _ = _semaphore.Release();
            }
        }

        private static bool ShouldUseEffectiveTokenDirectly(TimeSpan? budgetRemaining, TimeSpan perAttempt) => budgetRemaining is not null && perAttempt >= budgetRemaining.Value;

        private Task BackoffAsync(TimeSpan d, CancellationToken outerCt)
        {
            ServerCallPolicyMetrics.IncrementBackoffLabel(_peer, 1);
            ServerCallPolicyMetrics.ObserveBackoffSeconds(_peer, d);
            return Task.Delay(d, _timeProvider, outerCt);
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
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
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
                var cancelKind = ServerCancelClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptToken);
                if (cancelKind is not ServerCancelScenarioKind.PerAttemptTimedOut || attempt >= _maxAttempts)
                    return AttemptOutcome<T>.Stop(oce);
                ServerRpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "operation_canceled").Inc();
                ServerCallPolicyMetrics.IncrementRetriesTotal(_peer, "operation_canceled");
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), oce, effectiveToken).ConfigureAwait(false));
            }
            catch (RpcException rx) when (rx.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded && attempt < _maxAttempts &&
                                          ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
            {
                ServerRpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", rx.StatusCode is StatusCode.DeadlineExceeded ? "deadline_exceeded" : "Canceled").Inc();
                ServerCallPolicyMetrics.IncrementRetriesTotal(_peer, rx.StatusCode is StatusCode.DeadlineExceeded ? "deadline_exceeded" : "Canceled");
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, effectiveToken).ConfigureAwait(false));
            }
            catch (RpcException rx) when (rx.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Internal or StatusCode.ResourceExhausted &&
                                          attempt < _maxAttempts && ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
            {
                if (rx.StatusCode is StatusCode.DeadlineExceeded)
                    ServerRpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "deadline_exceeded").Inc();

                ServerCallPolicyMetrics.IncrementRetriesTotal(_peer, ServerCallPolicyRetryClassifier.ClassifyRetryReason(rx));
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), rx, effectiveToken).ConfigureAwait(false));
            }
            catch (HttpRequestException ex) when (attempt < _maxAttempts && ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
            {
                ServerCallPolicyMetrics.IncrementRetriesTotal(_peer, ServerCallPolicyRetryClassifier.ClassifyRetryReason(ex));
                return AttemptOutcome<T>.Retry(await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), ex, effectiveToken).ConfigureAwait(false));
            }
            catch (RpcException rx)
            {
                return AttemptOutcome<T>.Stop(rx);
            }
            catch (HttpRequestException ex)
            {
                return AttemptOutcome<T>.Stop(ex);
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

        private async ValueTask<T> RunRetryLoopAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            bool hasDeadlineBudget,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var attempt = 0;
            Exception? last = null;

            while (ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts)
            {
                attempt++;
                var outcome = await TryOneAttemptAsync(state, action, attempt, effectiveToken, cancellationToken).ConfigureAwait(false);
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
            if (!hasDeadlineBudget || ServerCancelClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
            {
                throw last switch
                {
                    TaskCanceledException or OperationCanceledException => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts timed out.")),
                    RpcException { StatusCode: StatusCode.Cancelled } => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts Canceled by per-attempt timeout.")),
                    _ => last!,
                };
            }

            ServerRpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
        }

        private void ThrowIfDraining()
        {
            if (!_isDraining())
                return;

            ServerCallPolicyMetrics.IncrementDrainRejectsTotal(_peer, 1);
            throw new RpcException(new Status(StatusCode.Unavailable, "ServerPeer client pool is draining."));
        }

        private async ValueTask<AttemptOutcome<T>> TryOneAttemptAsync<TState, T>(
            TState state,
            Func<TState, CancellationToken, ValueTask<T>> action,
            int attempt,
            CancellationToken effectiveToken,
            CancellationToken cancellationToken)
        {
            var budgetRemaining = ServerRpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
            var perAttempt = GetAttemptTimeoutForRemaining(budgetRemaining);
            if (ShouldUseEffectiveTokenDirectly(budgetRemaining, perAttempt))
                return await ExecuteAttemptCoreAsync(state, action, attempt, effectiveToken, cancellationToken, effectiveToken).ConfigureAwait(false);

            if (effectiveToken.CanBeCanceled)
            {
                using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveToken);
                if (budgetRemaining is null || perAttempt < budgetRemaining.Value)
                    attemptCts.CancelAfter(perAttempt);

                return await ExecuteAttemptCoreAsync(state, action, attempt, effectiveToken, cancellationToken, attemptCts.Token).ConfigureAwait(false);
            }

            using var standaloneAttemptCts = new CancellationTokenSource();
            if (budgetRemaining is null || perAttempt < budgetRemaining.Value)
                standaloneAttemptCts.CancelAfter(perAttempt);

            return await ExecuteAttemptCoreAsync(state, action, attempt, effectiveToken, cancellationToken, standaloneAttemptCts.Token).ConfigureAwait(false);
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

        private static class ServerCallPolicyRetryClassifier
        {
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
                StatusCode.Cancelled => "Canceled",
                StatusCode.DeadlineExceeded => "deadline_exceeded",
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
