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
/// An absolute-deadline overload shares one deadline between the reroute and the per-endpoint transport
/// retries instead of multiplying retry counters across layers.
/// </remarks>
internal sealed class EndpointFailover
{
    private const string StaleTermDetail = "stale-term";

    private readonly Lock _activeIndexGate = new();
    private readonly IReadOnlyList<string> _bootstrapNodeIds;
    private int _activeIndex;

    internal EndpointFailover(IReadOnlyList<string> bootstrapNodeIds, string primaryNodeId)
    {
        ArgumentNullException.ThrowIfNull(bootstrapNodeIds);
        if (bootstrapNodeIds.Count == 0)
            throw new ArgumentException("At least one bootstrap node id is required.", nameof(bootstrapNodeIds));

        _bootstrapNodeIds = bootstrapNodeIds;
        _activeIndex = ResolveActiveIndex(bootstrapNodeIds, primaryNodeId);
    }

    internal ValueTask<TResult> ExecuteAsync<TResult>(Func<string, CancellationToken, ValueTask<TResult>> action, CancellationToken cancellationToken) => ExecuteAsync(
        static (nodeId, callback, token) => callback(nodeId, token),
        action,
        cancellationToken);

    internal ValueTask<TResult> ExecuteAsync<TState, TResult>(
        Func<string, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        return ExecuteCoreAsync(action, state, null, null, cancellationToken);
    }

    /// <summary>
    /// Executes the action with at most one stale-term reroute under a single absolute deadline shared
    /// with the per-endpoint transport retries.
    /// </summary>
    /// <typeparam name="TState">The logical operation state, preserved across the reroute.</typeparam>
    /// <typeparam name="TResult">The operation result type.</typeparam>
    /// <param name="action">The endpoint-bound operation.</param>
    /// <param name="state">The logical operation state, including the operation id.</param>
    /// <param name="overallDeadline">The single budget for the reroute and all transport retries.</param>
    /// <param name="timeProvider">The time source reading the clock; <see langword="null" /> selects <see cref="TimeProvider.System" />.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The operation result.</returns>
    internal async ValueTask<TResult> ExecuteWithDeadlineAsync<TState, TResult>(
        Func<string, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        TimeSpan overallDeadline,
        TimeProvider? timeProvider,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(action);

        var clock = timeProvider ?? TimeProvider.System;
        var deadlineUtc = clock.GetUtcNow() + overallDeadline;
        using var deadlineScope = RpcDeadlineContext.Push(deadlineUtc.UtcDateTime);
        return await ExecuteCoreAsync(action, state, clock, deadlineUtc, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>
    /// Determines whether <paramref name="ex" /> reports an ambiguous durable commit outcome that must never
    /// fail over: the mutation may already be committed on this endpoint, and the next endpoint holds no
    /// idempotency record that would gate a re-execution.
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> when the status detail is the exact stable ambiguous-commit contract.</returns>
    private static bool IsCommitOutcomeUnknown(RpcException ex) =>
        string.Equals(ex.Status.Detail, CommitOutcomeUnknownException.StableDetail, StringComparison.Ordinal);

    /// <summary>
    /// Determines whether <paramref name="ex" /> reports a stale term from a deposed endpoint.
    /// The detail mirrors the server refusal marker so the client reroutes without referencing the server assembly.
    /// </summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> for a stale-term response; otherwise <see langword="false" />.</returns>
    private static bool IsStaleTerm(RpcException ex) =>
        ex.StatusCode == StatusCode.FailedPrecondition && string.Equals(ex.Status.Detail, StaleTermDetail, StringComparison.Ordinal);

    /// <summary>Determines whether the shared absolute deadline already passed.</summary>
    /// <param name="clock">The time source reading the clock.</param>
    /// <param name="deadlineUtc">The single absolute deadline; <see langword="null" /> means unbounded.</param>
    /// <returns><see langword="true" /> when a deadline is set and already passed; otherwise <see langword="false" />.</returns>
    private static bool IsExpired(TimeProvider? clock, DateTimeOffset? deadlineUtc)
    {
        if (clock == null || deadlineUtc == null)
            return false;

        return clock.GetUtcNow() >= deadlineUtc.Value;
    }

    /// <summary>Determines whether <paramref name="ex" /> authorizes the single stale-term reroute.</summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <param name="rerouted">Whether the single reroute was already consumed.</param>
    /// <param name="hasMoreEndpoints">Whether another endpoint remains to reroute to.</param>
    /// <returns><see langword="true" /> when a stale-term reroute may proceed; otherwise <see langword="false" />.</returns>
    private static bool IsRetryableStaleTerm(RpcException ex, bool rerouted, bool hasMoreEndpoints) =>
        IsStaleTerm(ex) && !rerouted && hasMoreEndpoints;

    /// <summary>Determines whether <paramref name="ex" /> is a retryable transport failure.</summary>
    /// <param name="ex">The gRPC transport exception from the attempted endpoint.</param>
    /// <returns><see langword="true" /> for retryable transport status codes; otherwise <see langword="false" />.</returns>
    private static bool IsRetryableTransport(RpcException ex) =>
        ex.StatusCode == StatusCode.Unavailable ||
        ex.StatusCode == StatusCode.DeadlineExceeded ||
        ex.StatusCode == StatusCode.Internal ||
        ex.StatusCode == StatusCode.ResourceExhausted;

    /// <summary>Throws when the shared absolute deadline passed without a captured endpoint failure.</summary>
    /// <param name="clock">The time source reading the clock.</param>
    /// <param name="deadlineUtc">The single absolute deadline; <see langword="null" /> means unbounded.</param>
    /// <exception cref="RpcException">Thrown when the deadline passed.</exception>
    private static void ThrowIfExpired(TimeProvider? clock, DateTimeOffset? deadlineUtc)
    {
        if (IsExpired(clock, deadlineUtc))
            throw new RpcException(new Status(StatusCode.DeadlineExceeded, "Bootstrap failover deadline exceeded."));
    }

    private static int ResolveActiveIndex(IReadOnlyList<string> bootstrapNodeIds, string primaryNodeId)
    {
        for (var i = 0; i < bootstrapNodeIds.Count; i++)
        {
            if (string.Equals(bootstrapNodeIds[i], primaryNodeId, StringComparison.Ordinal))
                return i;
        }

        throw new InvalidOperationException("Bootstrap primary node is not configured.");
    }

    private async ValueTask<TResult> ExecuteCoreAsync<TState, TResult>(
        Func<string, TState, CancellationToken, ValueTask<TResult>> action,
        TState state,
        TimeProvider? clock,
        DateTimeOffset? deadlineUtc,
        CancellationToken cancellationToken)
    {
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

        if (lastFailure == null)
            ThrowIfExpired(clock, deadlineUtc);

        throw lastFailure ?? new InvalidOperationException("Bootstrap endpoint failover failed without a captured exception.");
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
