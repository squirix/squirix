using System;
using System.Diagnostics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Node;
using Squirix.Server.Node.Observability;

namespace Squirix.Server.Cluster.Reliability;

internal sealed class CallPolicy : ICallPolicy
{
    private readonly TimeSpan _baseBackoff;
    private readonly Lock _disposeGate = new();
    private readonly int _maxAttempts;
    private readonly TimeSpan _maxBackoff;
    private readonly string _peer;
    private readonly SemaphoreSlim _semaphore;
    private readonly TimeSpan _timeoutPerAttempt;
    private int _activeOperations;
    private bool _disposed;
    private Task? _disposeTask;
    private TaskCompletionSource<bool>? _disposeTcs;
    private volatile bool _draining;
    private bool _semaphoreDisposed;

    public CallPolicy(
        TimeSpan? timeoutPerAttempt = null,
        int maxAttempts = 3,
        TimeSpan? baseBackoff = null,
        TimeSpan? maxBackoff = null,
        int maxConcurrentPerPeer = 64,
        string? peer = null)
    {
        _peer = string.IsNullOrWhiteSpace(peer) ? "unknown" : peer;
        _timeoutPerAttempt = timeoutPerAttempt ?? TimeSpan.FromMilliseconds(600);
        _maxAttempts = Math.Max(1, maxAttempts);
        _baseBackoff = baseBackoff ?? TimeSpan.FromMilliseconds(50);
        _maxBackoff = maxBackoff ?? TimeSpan.FromMilliseconds(500);

        var cap = Math.Max(1, maxConcurrentPerPeer);
        _semaphore = new SemaphoreSlim(cap, cap);
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
            if (Volatile.Read(ref _activeOperations) == 0)
            {
                TryDisposeSemaphoreUnderLock();
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

    public async ValueTask<T> ExecuteAsync<T>(Func<CancellationToken, ValueTask<T>> action, CancellationToken cancellationToken)
    {
        ThrowIfDisposed();
        ThrowIfDraining();

        _ = Interlocked.Increment(ref _activeOperations);

        try
        {
            cancellationToken.ThrowIfCancellationRequested(); // Ensure we never continue with a canceled token

            using var budgetCts = CreateBudgetTokenSource(cancellationToken, out var hasDeadlineBudget);
            var effectiveToken = budgetCts?.Token ?? cancellationToken;

            var queueWaitStarted = Stopwatch.GetTimestamp();
            await _semaphore.WaitAsync(effectiveToken).ConfigureAwait(false);
            CallPolicyMetrics.QueueWaitSeconds.Observe(_peer, Stopwatch.GetElapsedTime(queueWaitStarted));
            try
            {
                ThrowIfDraining();

                var attempt = 0;
                Exception? last = null;

                while (OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken) && attempt < _maxAttempts)
                {
                    attempt++;

                    using var attemptCts = CancellationTokenSource.CreateLinkedTokenSource(effectiveToken);
                    ConfigurePerAttemptTimeout(attemptCts);

                    try
                    {
                        return await action(attemptCts.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException oce)
                    {
                        var cancelKind = OperationCancellationClassifier.ClassifyPeerCallAttemptCancellation(cancellationToken, effectiveToken, attemptCts.Token);
                        if (cancelKind == CancellationScenarioKind.PerAttemptTimedOut && attempt < _maxAttempts)
                        {
                            // Per-attempt timeout: treat as retryable
                            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "operation_canceled").Inc();
                            CallPolicyMetrics.RetriesTotal.WithLabels(_peer, "operation_canceled").Inc(1);
                            last = oce;
                            last = await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), last, effectiveToken).ConfigureAwait(false);
                            continue;
                        }

                        // Caller cancel, budget deadline, unknown cancel, or final non-retryable attempt: capture and exit the retry loop.
                        last = oce;
                        break;
                    }
                    catch (RpcException rx) when (rx.StatusCode is StatusCode.Cancelled or StatusCode.DeadlineExceeded && attempt < _maxAttempts &&
                                                  OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                    {
                        RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", rx.StatusCode == StatusCode.DeadlineExceeded ? "deadline_exceeded" : "cancelled").Inc();
                        CallPolicyMetrics.RetriesTotal.WithLabels(_peer, rx.StatusCode == StatusCode.DeadlineExceeded ? "deadline_exceeded" : "cancelled").Inc(1);
                        last = rx;
                        last = await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), last, effectiveToken).ConfigureAwait(false);
                    }
                    catch (RpcException rx) when (rx.StatusCode is StatusCode.Unavailable or StatusCode.DeadlineExceeded or StatusCode.Internal or StatusCode.ResourceExhausted &&
                                                  attempt < _maxAttempts &&
                                                  OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                    {
                        // Transient issue: retry with backoff
                        if (rx.StatusCode == StatusCode.DeadlineExceeded)
                            RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "attempt", "deadline_exceeded").Inc();

                        CallPolicyMetrics.RetriesTotal.WithLabels(_peer, ClassifyRetryReason(rx)).Inc(1);
                        last = rx;
                        last = await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), last, effectiveToken).ConfigureAwait(false);
                    }
                    catch (HttpRequestException ex) when (attempt < _maxAttempts &&
                                                          OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                    {
                        CallPolicyMetrics.RetriesTotal.WithLabels(_peer, ClassifyRetryReason(ex)).Inc(1);
                        last = ex;
                        last = await BackoffOrCaptureCancellationAsync(BackoffWithJitter(attempt), last, effectiveToken).ConfigureAwait(false);
                    }
                    catch (RpcException rx)
                    {
                        last = rx;
                        break;
                    }
                    catch (HttpRequestException ex)
                    {
                        last = ex;
                        break;
                    }
                }

                // If the outer token was cancelled, throw the standard OCE tied to that token.
                cancellationToken.ThrowIfCancellationRequested();
                if (!hasDeadlineBudget || OperationCancellationClassifier.OperationEffectiveTokenAllowsRetryAttempt(effectiveToken))
                {
                    throw last switch
                    {
                        // From here on, we must throw a non-null exception.
                        TaskCanceledException or OperationCanceledException => new RpcException(new Status(StatusCode.DeadlineExceeded, "All attempts timed out.")),
                        RpcException { StatusCode: StatusCode.Cancelled } => new RpcException(
                            new Status(StatusCode.DeadlineExceeded, "All attempts cancelled by per-attempt timeout.")),
                        _ => last!,
                    };
                }

                RpcTimeoutMetrics.TimeoutsTotal.WithLabels(_peer, "overall", "deadline_budget").Inc();
                throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Request deadline exceeded."));
            }
            finally
            {
                _ = _semaphore.Release();
            }
        }
        finally
        {
            ReleaseActiveOperation();
        }
    }

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
        StatusCode.Cancelled => "cancelled",
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

    private static CancellationTokenSource? CreateBudgetTokenSource(CancellationToken cancellationToken, out bool hasDeadlineBudget)
    {
        var remaining = RpcDeadlineContext.GetRemainingBudget(DateTime.UtcNow);
        if (remaining is null)
        {
            hasDeadlineBudget = false;
            return null;
        }

        hasDeadlineBudget = true;
        if (remaining <= TimeSpan.Zero)
        {
            var expired = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            expired.Cancel();
            return expired;
        }

        var cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        cts.CancelAfter(remaining.Value);
        return cts;
    }

    private async Task BackoffAsync(TimeSpan d, CancellationToken outerCt)
    {
        CallPolicyMetrics.BackoffsTotal.WithLabels(_peer).Inc(1);
        CallPolicyMetrics.BackoffSeconds.Observe(_peer, d);
        await Task.Delay(d, outerCt).ConfigureAwait(false);
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
        var jitterFactor = 0.5 + (Random.Shared.NextDouble() * 0.5);
        var candidateMs = cappedMs * jitterFactor;

        // Enforce a small floor (50ms) per backoff when cap permits, to avoid flaky sub-50ms totals
        var floorMs = Math.Min(50.0, cappedMs);
        var finalMs = Math.Max(candidateMs, floorMs);

        return TimeSpan.FromMilliseconds(finalMs);
    }

    /// <summary>
    /// When the per-attempt cap matches the remaining RPC deadline, scheduling a second per-attempt timeout on the
    /// linked source races the budget token and can classify the same wall-clock timeout as a per-attempt cancel.
    /// </summary>
    private void ConfigurePerAttemptTimeout(CancellationTokenSource attemptCts)
    {
        var now = DateTime.UtcNow;
        var budgetRemaining = RpcDeadlineContext.GetRemainingBudget(now);
        var perAttempt = GetAttemptTimeoutForRemaining(budgetRemaining);

        // No separate timer when the attempt cap is not stricter than the ambient budget (linking is enough).
        if (budgetRemaining is null || perAttempt < budgetRemaining.Value)
            attemptCts.CancelAfter(perAttempt);
    }

    private TimeSpan GetAttemptTimeoutForRemaining(TimeSpan? remaining) => remaining is null ? _timeoutPerAttempt :
        remaining <= TimeSpan.Zero ? TimeSpan.Zero :
        remaining.Value < _timeoutPerAttempt ? remaining.Value : _timeoutPerAttempt;

    private void ReleaseActiveOperation()
    {
        if (Interlocked.Decrement(ref _activeOperations) != 0)
            return;

        lock (_disposeGate)
            TryDisposeSemaphoreUnderLock();
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

        CallPolicyMetrics.DrainRejectsTotal.WithLabels(_peer).Inc(1);
        throw new RpcException(new Status(StatusCode.Unavailable, "Peer client pool is draining."));
    }

    private void TryDisposeSemaphoreUnderLock()
    {
        if (!_disposed || _semaphoreDisposed || Volatile.Read(ref _activeOperations) != 0)
            return;

        _semaphore.Dispose();
        _semaphoreDisposed = true;
        _ = _disposeTcs?.TrySetResult(true);
    }
}
