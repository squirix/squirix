using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Runs an inbound single-key RPC, before any idempotency or pipeline work, on this node or on the leader of the key's replica group.</summary>
/// <remarks>
/// The group of a key is named by its ring owner. Its leader comes from the leader table: with a static table the ring owner always leads,
/// so the call runs locally or is forwarded once to the owner, and every refusal is relayed. With an election-led table the router waits
/// for a leader within the deadline, and when the chosen route answers as stale (nothing was appended), refutes it and reroutes once with
/// the same request, so with the same operation id; a second stale answer ends the operation as <see cref="StaleRouteSignals.LeaderChanged" />.
/// Transport failures, an unknown commit outcome, and every other failure are never rerouted.
/// </remarks>
[Immutable]
internal sealed class OwnerRouter
{
    private readonly TimeProvider _clock;
    private readonly IRemoteInvocationState _invocationState;
    private readonly TimeSpan _leaderWait;
    private readonly INodeOwnershipResolver _ownershipResolver;
    private readonly RingAgreement _ringAgreement;
    private readonly IGroupLeaderTable _table;

    /// <summary>Initializes a new instance of the <see cref="OwnerRouter" /> class.</summary>
    /// <param name="ownershipResolver">Resolves the ring owner of a key, which names its replica group.</param>
    /// <param name="invocationState">Tells whether the current call is a trusted internal owner RPC.</param>
    /// <param name="ringAgreement">Fences this node on a ring mismatch.</param>
    /// <param name="table">The leader of every group as this node knows it.</param>
    /// <param name="leaderWait">The longest wait for a leader of a served group that has none known; the remaining deadline caps it further.</param>
    /// <param name="clock">The clock the deadline of an operation counts down on.</param>
    internal OwnerRouter(
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState,
        RingAgreement ringAgreement,
        IGroupLeaderTable table,
        TimeSpan leaderWait,
        TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(ownershipResolver);
        ArgumentNullException.ThrowIfNull(invocationState);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(leaderWait, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(clock);
        _ownershipResolver = ownershipResolver;
        _invocationState = invocationState;
        _ringAgreement = ringAgreement;
        _table = table;
        _leaderWait = leaderWait;
        _clock = clock;
    }

    /// <summary>Runs a single-key call on this node or forwards it to the leader of the key's group.</summary>
    /// <typeparam name="TState">The state the call delegates read.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="cacheName">The cache name from the request.</param>
    /// <param name="key">The key from the request.</param>
    /// <param name="state">The state passed to <paramref name="forward" /> and <paramref name="local" />.</param>
    /// <param name="forward">Forwards the call to the node it names; one internode attempt.</param>
    /// <param name="local">Runs the call on this node.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The response of the attempt that answered.</returns>
    /// <remarks>An invalid cache name or key runs on this node, so the canonical validation error is raised as for any local call.</remarks>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.Unavailable" /> with the ring-fenced trailer when this node detected a ring mismatch with a peer; or
    /// <see cref="StatusCode.FailedPrecondition" /> with the stale-owner trailer when a trusted internal owner RPC reaches a node that does not lead
    /// the key's group; or <see cref="StatusCode.Unavailable" /> when no leader is known within the wait, or the route went stale again after the
    /// single reroute.
    /// </exception>
    internal Task<TResponse> ExecuteAsync<TState, TResponse>(
        string cacheName,
        string key,
        TState state,
        Func<TState, string, CancellationToken, Task<TResponse>> forward,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(forward);
        ArgumentNullException.ThrowIfNull(local);
        _ringAgreement.EnsureNotFenced();
        if (!ServerCacheName.TryParsePublic(cacheName, out var canonicalName) || !CacheKeyValidator.TryValidate(key, out _))
            return local(state, cancellationToken);

        var groupId = _ownershipResolver.GetOwner(canonicalName, key);
        var route = ResolveNow(groupId);

        // An internal call is itself the single hop: it runs here or is refused. A static table never refutes a route, so a reroute could
        // only repeat the refused attempt: one attempt, its refusal relayed.
        return (Internal: _invocationState.IsInternalOwnerInvocation, Static: _table is StaticLeaderTable) switch
        {
            (true, _) when !IsSelf(in route) => throw RefuseInternal(in route),
            (true, _) or (_, true) => AttemptAsync(in route, state, forward, local, cancellationToken),
            _ => RouteAsync(groupId, route, state, forward, local, cancellationToken),
        };
    }

    private Task<TResponse> AttemptAsync<TState, TResponse>(
        in LeaderRoute route,
        TState state,
        Func<TState, string, CancellationToken, Task<TResponse>> forward,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken) => IsSelf(in route) ? local(state, cancellationToken) : forward(state, route.NodeId, cancellationToken);

    private bool IsSelf(in LeaderRoute route) => string.Equals(route.NodeId, _ownershipResolver.SelfNodeId, StringComparison.Ordinal);

    /// <summary>Refuses a trusted internal owner RPC that reached a node without authority over the key's group; it is never forwarded again.</summary>
    /// <param name="route">The leader this node knows; <see langword="default" /> when none.</param>
    /// <returns>The stale-owner refusal naming the known leader, or the no-leader refusal.</returns>
    private RpcException RefuseInternal(in LeaderRoute route) => string.IsNullOrEmpty(route.NodeId)
        ? ServerOpContract.NoLeaderAuthority()
        : StaleOwnerFailure.Create(route.NodeId, _ownershipResolver.SelfNodeId);

    /// <summary>Resolves the route of a group without waiting.</summary>
    /// <param name="groupId">The group, named by the ring owner of the key.</param>
    /// <returns>The known leader; the ring owner in term zero for a group this node does not serve; otherwise <see langword="default" />.</returns>
    /// <remarks>
    /// A group this node holds no election state for goes to its ring owner, a member that names the leader when it does not lead. The table
    /// reports <see langword="default" /> when it knows no leader.
    /// </remarks>
    private LeaderRoute ResolveNow(string groupId) =>
        _table.TryGetLeader(groupId, out var route) || _table.Read(groupId).Served ? route : new LeaderRoute(groupId, 0);

    /// <summary>Resolves the route of a group, waiting for a leader within the budget and the leader wait when none is known.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="budget">The budget of the operation.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The route.</returns>
    /// <exception cref="RpcException"><see cref="StatusCode.Unavailable" />: no leader became known within the wait.</exception>
    private async ValueTask<LeaderRoute> ResolveAsync(string groupId, RerouteBudget budget, CancellationToken cancellationToken)
    {
        var route = ResolveNow(groupId);
        if (!string.IsNullOrEmpty(route.NodeId))
            return route;

        var remaining = budget.GetRemaining();
        var wait = remaining < _leaderWait ? remaining : _leaderWait;
        if (wait < TimeSpan.Zero)
            wait = TimeSpan.Zero;

        return await _table.WaitForLeaderAsync(groupId, wait, cancellationToken).ConfigureAwait(false) && _table.TryGetLeader(groupId, out route)
            ? route
            : throw ServerOpContract.NoLeaderAuthority();
    }

    /// <summary>Refutes a route that answered as stale and picks the route of the single reroute.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="refused">The route that answered as stale.</param>
    /// <param name="hint">The leader the refusing node named; <see langword="default" /> when none.</param>
    /// <param name="budget">The budget of the operation.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The next route, never <paramref name="refused" />.</returns>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.Unavailable" />: the reroute was already spent, the deadline passed, no other route is known, or no leader became
    /// known within the wait.
    /// </exception>
    private async ValueTask<LeaderRoute> RerouteAsync(string groupId, LeaderRoute refused, LeaderRoute hint, RerouteBudget budget, CancellationToken cancellationToken)
    {
        _table.Refute(groupId, in refused);
        if (!budget.TryConsumeReroute() || budget.HasExpired())
            throw StaleRouteSignals.LeaderChanged();

        // The hint of the refusing node comes first; this node decides its own authority from its table, never from a hint.
        var next = !string.IsNullOrEmpty(hint.NodeId) && !IsSelf(in hint) && !string.Equals(hint.NodeId, refused.NodeId, StringComparison.Ordinal)
            ? hint
            : await ResolveAsync(groupId, budget, cancellationToken).ConfigureAwait(false);
        return string.Equals(next.NodeId, refused.NodeId, StringComparison.Ordinal) ? throw StaleRouteSignals.LeaderChanged() : next;
    }

    /// <summary>Runs the call on the leader of a group, rerouting once when the route answers as stale.</summary>
    /// <typeparam name="TState">The state the call delegates read.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="groupId">The group.</param>
    /// <param name="known">The route resolved without waiting; <see langword="default" /> when none is known yet.</param>
    /// <param name="state">The call state.</param>
    /// <param name="forward">Forwards the call.</param>
    /// <param name="local">Runs the call on this node.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The response of the attempt that answered.</returns>
    /// <remarks>The budget allows one reroute, so at most two attempts run: <see cref="RerouteAsync" /> refuses a second one.</remarks>
    private async Task<TResponse> RouteAsync<TState, TResponse>(
        string groupId,
        LeaderRoute known,
        TState state,
        Func<TState, string, CancellationToken, Task<TResponse>> forward,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken)
    {
        var budget = RerouteBudget.FromRemaining(ServerRpcDeadlineContext.GetRemainingBudget(), _clock);
        var route = string.IsNullOrEmpty(known.NodeId) ? await ResolveAsync(groupId, budget, cancellationToken).ConfigureAwait(false) : known;
        while (true)
        {
            try
            {
                return await AttemptAsync(in route, state, forward, local, cancellationToken).ConfigureAwait(false);
            }
            catch (RpcException ex) when (StaleRouteSignals.TryReadStale(ex, out var hint))
            {
                route = await RerouteAsync(groupId, route, hint, budget, cancellationToken).ConfigureAwait(false);
            }
        }
    }
}
