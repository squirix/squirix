using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal.Cluster.Observability;

namespace Squirix.Internal;

/// <summary>Routes single-node remote cache calls across bootstrap endpoints, failing over on transport-level errors.</summary>
/// <remarks>
/// A stale-term response reroutes at most once to another endpoint: the deposed endpoint may still
/// serve stale state, but the operation never bounces between endpoints. The logical state (including
/// the operation id) is passed through unchanged, so a rerouted mutation keeps its idempotency record.
/// Every operation runs under one absolute deadline: only its duration is fixed at construction, the instant is set
/// when the operation starts. It is shared between the reroute and the per-endpoint transport retries instead of
/// multiplying retry counters across layers, and it is published through <see cref="RpcDeadlineContext" /> so
/// outgoing calls carry it as the gRPC deadline.
/// </remarks>
internal sealed class EndpointFailover
{
    private const string StaleTermDetail = "stale-term";

    private readonly Lock _activeIndexGate = new();
    private readonly IReadOnlyList<string> _bootstrapNodeIds;
    private readonly TimeSpan _operationDeadline;
    private readonly TimeProvider _timeProvider;
    private int _activeIndex;

    /// <summary>Initializes a new instance of the <see cref="EndpointFailover" /> class.</summary>
    /// <param name="bootstrapNodeIds">The bootstrap endpoint node ids in failover order.</param>
    /// <param name="primaryNodeId">The node id of the endpoint tried first.</param>
    /// <param name="operationDeadline">The finite positive duration each operation may take in total.</param>
    /// <param name="timeProvider">
    /// The clock used to compute the deadline. It must track wall-clock UTC: the pushed absolute deadline is compared
    /// against <see cref="DateTime.UtcNow" /> by the call policy and by the gRPC client.
    /// </param>
    internal EndpointFailover(IReadOnlyList<string> bootstrapNodeIds, string primaryNodeId, TimeSpan operationDeadline, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(bootstrapNodeIds);
        ArgumentNullException.ThrowIfNull(timeProvider);
        if (bootstrapNodeIds.Count == 0)
            throw new ArgumentException("At least one bootstrap node id is required.", nameof(bootstrapNodeIds));
        if (operationDeadline <= TimeSpan.Zero || operationDeadline == Timeout.InfiniteTimeSpan)
            throw new ArgumentOutOfRangeException(nameof(operationDeadline), operationDeadline, "The operation deadline must be a finite positive duration.");

        _bootstrapNodeIds = bootstrapNodeIds;
        _operationDeadline = operationDeadline;
        _timeProvider = timeProvider;
        _activeIndex = ResolveActiveIndex(bootstrapNodeIds, primaryNodeId);
    }

    /// <summary>
    /// Executes the action with at most one stale-term reroute under the single absolute operation deadline shared
    /// with the per-endpoint transport retries.
    /// </summary>
    /// <typeparam name="TState">The logical operation state, preserved across the reroute.</typeparam>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="action">The endpoint-bound operation.</param>
    /// <param name="state">The logical operation state, including the operation id.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The operation result.</returns>
    internal async ValueTask<TResult> ExecuteAsync<TState, TResult>(Func<string, TState, CancellationToken, ValueTask<TResult>> action, TState state, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var clock = _timeProvider;
        var deadlineUtc = clock.GetUtcNow() + _operationDeadline;
        using var deadlineScope = RpcDeadlineContext.Push(deadlineUtc.UtcDateTime, clock);
        var startIndex = ActiveIndexSnapshot();
        Exception? lastFailure = null;
        var rerouted = false;

        for (var attempt = 0; attempt < _bootstrapNodeIds.Count; attempt++)
        {
            if (IsExpired(clock, deadlineUtc))
                break;

            var nodeIndex = (startIndex + attempt) % _bootstrapNodeIds.Count;
            var nodeId = _bootstrapNodeIds[nodeIndex];
            var hasMoreEndpoints = attempt < _bootstrapNodeIds.Count - 1;

            try
            {
                var result = await action(nodeId, state, cancellationToken).ConfigureAwait(false);
                if (nodeIndex != startIndex)
                    SetActiveIndex(nodeIndex);

                return result;
            }
            catch (RpcException ex) when (IsRetryableStaleTerm(ex, rerouted, hasMoreEndpoints))
            {
                rerouted = true;
                lastFailure = ex;
            }
            catch (RpcException ex) when (IsRetryableTransport(ex) && !IsCommitOutcomeUnknown(ex) && hasMoreEndpoints)
            {
                lastFailure = ex;
            }
            catch (HttpRequestException ex) when (hasMoreEndpoints)
            {
                lastFailure = ex;
            }
            catch (IOException ex) when (hasMoreEndpoints)
            {
                lastFailure = ex;
            }
        }

        // Defensive: with no captured failure the loop ended before its first attempt, which only happens when the clock moved past the
        // deadline between the start of the operation and that attempt.
        if (lastFailure == null)
            ThrowIfExpired(clock, deadlineUtc, _bootstrapNodeIds[startIndex]);

        throw lastFailure ?? new InvalidOperationException("Bootstrap endpoint failover failed without a captured exception.");
    }

    /// <summary>
    /// Determines whether <paramref name="ex" /> reports an ambiguous durable commit outcome that must never
    /// fail over: the mutation may already be committed on this endpoint, and the next endpoint holds no
    /// idempotency record that would gate a re-execution.
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> when the status detail is the exact stable ambiguous-commit contract.</returns>
    private static bool IsCommitOutcomeUnknown(RpcException ex) => string.Equals(ex.Status.Detail, CommitOutcomeUnknownException.StableDetail, StringComparison.Ordinal);

    /// <summary>Determines whether the shared absolute deadline already passed.</summary>
    /// <param name="clock">The time source reading the clock.</param>
    /// <param name="deadlineUtc">The single absolute deadline.</param>
    /// <returns><see langword="true" /> when the deadline already passed; otherwise <see langword="false" />.</returns>
    private static bool IsExpired(TimeProvider clock, DateTimeOffset deadlineUtc) => clock.GetUtcNow() >= deadlineUtc;

    /// <summary>Determines whether <paramref name="ex" /> authorizes the single stale-term reroute.</summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <param name="rerouted">Whether the single reroute was already consumed.</param>
    /// <param name="hasMoreEndpoints">Whether another endpoint remains to reroute to.</param>
    /// <returns><see langword="true" /> when a stale-term reroute may proceed; otherwise <see langword="false" />.</returns>
    private static bool IsRetryableStaleTerm(RpcException ex, bool rerouted, bool hasMoreEndpoints) => IsStaleTerm(ex) && !rerouted && hasMoreEndpoints;

    /// <summary>Determines whether <paramref name="ex" /> is a retryable transport failure.</summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> for retryable transport status codes; otherwise <see langword="false" />.</returns>
    private static bool IsRetryableTransport(RpcException ex) => ex.StatusCode == StatusCode.Unavailable || ex.StatusCode == StatusCode.DeadlineExceeded ||
                                                                 ex.StatusCode == StatusCode.Internal || ex.StatusCode == StatusCode.ResourceExhausted;

    /// <summary>
    /// Determines whether <paramref name="ex" /> reports a stale term from a deposed endpoint.
    /// The detail mirrors the server refusal marker so the client reroutes without referencing the server assembly.
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> for a stale-term response; otherwise <see langword="false" />.</returns>
    private static bool IsStaleTerm(RpcException ex) =>
        ex.StatusCode == StatusCode.FailedPrecondition && string.Equals(ex.Status.Detail, StaleTermDetail, StringComparison.Ordinal);

    private static int ResolveActiveIndex(IReadOnlyList<string> bootstrapNodeIds, string primaryNodeId)
    {
        for (var i = 0; i < bootstrapNodeIds.Count; i++)
        {
            if (string.Equals(bootstrapNodeIds[i], primaryNodeId, StringComparison.Ordinal))
                return i;
        }

        throw new InvalidOperationException("Bootstrap primary node is not configured.");
    }

    /// <summary>Throws when the shared absolute deadline passed without a captured endpoint failure.</summary>
    /// <param name="clock">The time source reading the clock.</param>
    /// <param name="deadlineUtc">The single absolute deadline.</param>
    /// <param name="peer">The endpoint the operation would have used, recorded as the metric peer.</param>
    /// <exception cref="RpcException">Thrown when the deadline passed.</exception>
    private static void ThrowIfExpired(TimeProvider clock, DateTimeOffset deadlineUtc, string peer)
    {
        if (!IsExpired(clock, deadlineUtc))
            return;

        RpcTimeoutMetrics.TimeoutsTotal.WithLabels(peer, "overall", "deadline_budget").Inc();
        throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Bootstrap failover deadline exceeded."));
    }

    private int ActiveIndexSnapshot()
    {
        lock (_activeIndexGate)
            return _activeIndex;
    }

    private void SetActiveIndex(int nodeIndex)
    {
        lock (_activeIndexGate)
            _activeIndex = nodeIndex;
    }
}
