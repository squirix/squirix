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
using Squirix.Transport.Grpc;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Runs an inbound single-key RPC, before any idempotency or pipeline work, on this node or on the leader of the key's replica group.</summary>
/// <remarks>
/// The group of a key is named by its ring owner. Its leader comes from the leader table: with a static table the ring owner always leads,
/// so the call runs locally or is forwarded once to the owner, and every refusal is relayed. With an election-led table the router waits
/// for a leader within the deadline (a member stops routing to a leader it has not heard from for one election timeout and waits for the
/// next one), and when the chosen route answers as stale (nothing was appended), refutes it and reroutes once with
/// the same request, so with the same operation id: at most two logical attempts. A second stale answer ends the operation as
/// <see cref="ServerOpContract.LeaderChanged" />. For a group this node does not serve, a forward none of whose attempts connected to its target,
/// or that the target refused for having no leader, takes the single reroute to the leader learned for the group or to the next member in slot
/// order, and the leader a member names is learned for the next calls: a ring owner cut off from its group answers no leader while the other
/// members elect one. For a served group a failed connect instead waits for a known leader of a later term, without refuting the old route,
/// and spends the same single reroute on that replacement. Other transport failures, an unknown commit outcome, and every other failure are never rerouted; the call policy of a
/// forward may retry it on a transport failure, with the same request. A forwarded call that reaches a node leading the group without
/// authority yet, with no higher term seen, waits for that authority within the same bounds instead of being refused.
/// All the leader and authority waits of one call, on this node and on the nodes it is forwarded to, share one attempt budget (one attempt, or the
/// leader wait override plus <see cref="ElectionTimerOptions.LeaderRefusalMargin" />) counted from the arrival of the call at its entry node:
/// a forward carries the budget left as its deadline, and a wait for a leader ends at least <see cref="ElectionTimerOptions.LeaderRefusalMargin" />
/// before that budget or the caller deadline ends, and no reroute or fallback starts after that point. A forward started after a leader wait, a stale reroute or a fallback
/// keeps at least that margin, and its connect timeout is at most half of it, so a host that is down or drops connection attempts is refused as
/// owner unreachable before the deadline of that forward; a first forward with less deadline left than the connect timeout can still end as a deadline.
/// </remarks>
[Immutable]
internal sealed class OwnerRouter
{
    private readonly TimeSpan _attemptBudget;
    private readonly TimeProvider _clock;
    private readonly IRemoteInvocationState _invocationState;
    private readonly TimeSpan _leaderWait;
    private readonly INodeOwnershipResolver _ownershipResolver;
    private readonly IReplicaGroupLocator _replicaGroups;
    private readonly RingAgreement _ringAgreement;
    private readonly IGroupLeaderTable _table;

    /// <summary>Initializes a new instance of the <see cref="OwnerRouter" /> class.</summary>
    /// <param name="ownershipResolver">Resolves the ring owner of a key, which names its replica group.</param>
    /// <param name="invocationState">Tells whether the current call is a trusted internal owner RPC.</param>
    /// <param name="ringAgreement">Fences this node on a ring mismatch.</param>
    /// <param name="table">The leader of every group as this node knows it.</param>
    /// <param name="leaderWait">
    /// The longest single wait for a leader of a served group that has none known; the attempt budget of the call and the remaining deadline cap it further.
    /// </param>
    /// <param name="clock">The clock the deadline of an operation counts down on.</param>
    /// <param name="replicaGroups">Resolves the replica set of a group; a leader hint naming a node outside it is ignored.</param>
    internal OwnerRouter(
        INodeOwnershipResolver ownershipResolver,
        IRemoteInvocationState invocationState,
        RingAgreement ringAgreement,
        IGroupLeaderTable table,
        TimeSpan leaderWait,
        TimeProvider clock,
        IReplicaGroupLocator replicaGroups)
    {
        ArgumentNullException.ThrowIfNull(ownershipResolver);
        ArgumentNullException.ThrowIfNull(invocationState);
        ArgumentNullException.ThrowIfNull(ringAgreement);
        ArgumentNullException.ThrowIfNull(table);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(leaderWait, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(leaderWait, ElectionTimerOptions.MaxLeaderWaitTimeout);
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(replicaGroups);
        _ownershipResolver = ownershipResolver;
        _invocationState = invocationState;
        _ringAgreement = ringAgreement;
        _table = table;
        _leaderWait = leaderWait;
        _attemptBudget = leaderWait + ElectionTimerOptions.LeaderRefusalMargin > RpcAttemptTimeout.PerAttempt
            ? leaderWait + ElectionTimerOptions.LeaderRefusalMargin
            : RpcAttemptTimeout.PerAttempt;
        _clock = clock;
        _replicaGroups = replicaGroups;
    }

    /// <summary>Runs a single-key call on this node or forwards it to the leader of the key's group.</summary>
    /// <typeparam name="TState">The state the call delegates read.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="cacheName">The cache name from the request.</param>
    /// <param name="key">The key from the request.</param>
    /// <param name="state">The state passed to <paramref name="forward" /> and <paramref name="local" />.</param>
    /// <param name="forward">Forwards the call to the node it names: one logical attempt, which the call policy may retry on a transport failure.</param>
    /// <param name="local">Runs the call on this node.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The response of the attempt that answered.</returns>
    /// <remarks>An invalid cache name or key runs on this node, so the canonical validation error is raised as for any local call.</remarks>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.Unavailable" /> with the ring-fenced trailer when this node detected a ring mismatch with a peer; or
    /// <see cref="StatusCode.FailedPrecondition" /> with the stale-owner trailer when a trusted internal owner RPC reaches a node that does not lead
    /// the key's group, or the refusal of the group view when a node that leads it gets no authority within the wait; or <see cref="StatusCode.Unavailable" /> when no leader is known within the wait, the route went stale again after the
    /// single reroute, or a forward could not connect to its target, or its target had no leader, and no reroute was left.
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

        // An internal call is itself the single hop: it runs here or is refused. Elected right before it, this node may still wait for its own
        // authority. A static table never refutes a route, so a reroute could only repeat the refused attempt: one attempt, its refusal relayed.
        return (Internal: _invocationState.IsInternalOwnerInvocation, Static: _table is StaticLeaderTable) switch
        {
            (true, false) when !IsSelf(in route) && AwaitsAuthority(groupId) => AwaitAuthorityAsync(groupId, route, StartCall(), state, local, cancellationToken),
            (true, _) when !IsSelf(in route) => throw RefuseInternal(groupId, in route),
            (true, _) or (_, true) => AttemptAsync(in route, state, forward, local, cancellationToken),
            _ => RouteAsync(groupId, route, StartCall(), state, forward, local, cancellationToken),
        };
    }

    /// <summary>Gets the time left before the refusal of a call has to start so that it still reaches the caller.</summary>
    /// <param name="call">The attempt budget of the call.</param>
    /// <returns>The time left less <see cref="ElectionTimerOptions.LeaderRefusalMargin" />; negative once it passed.</returns>
    private static TimeSpan TimeToRefusal(in ServerDeadlineBudget call) => call.Remaining - ElectionTimerOptions.LeaderRefusalMargin;

    /// <summary>Tells whether an attempt was refused because its target knows no leader of the group with authority; nothing was read or written.</summary>
    /// <param name="exception">The failure of one attempt.</param>
    /// <returns><see langword="true" /> for the no-leader refusal, relayed from a forward's target.</returns>
    private static bool IsNoLeader(RpcException exception) => exception.StatusCode == StatusCode.Unavailable && string.Equals(
        exception.Status.Detail,
        ServerOpContract.NoLeaderAuthorityDetail,
        StringComparison.Ordinal);

    private Task<TResponse> AttemptAsync<TState, TResponse>(
        in LeaderRoute route,
        TState state,
        Func<TState, string, CancellationToken, Task<TResponse>> forward,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken) => IsSelf(in route) ? local(state, cancellationToken) : forward(state, route.NodeId, cancellationToken);

    /// <summary>Tells whether this node leads a group without authority yet and has seen no higher term, so it may still get the authority.</summary>
    /// <param name="groupId">The group.</param>
    /// <returns><see langword="true" /> when a trusted internal owner RPC of the group waits for this node's authority instead of being refused.</returns>
    private bool AwaitsAuthority(string groupId)
    {
        var view = _table.Read(groupId);
        return view.LedWithoutAuthority && view.HighestObservedTerm <= view.Term;
    }

    /// <summary>Runs a trusted internal owner RPC once this node, which leads the key's group without authority yet, gets it.</summary>
    /// <typeparam name="TState">The state the call delegates read.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="groupId">The group.</param>
    /// <param name="route">The route this node resolved; never this node.</param>
    /// <param name="call">The attempt budget of the call.</param>
    /// <param name="state">The call state.</param>
    /// <param name="local">Runs the call on this node.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The response of the local attempt.</returns>
    /// <remarks>
    /// A follower forwards to a new leader as soon as it accepts the leader-term entry, so the call usually arrives before that entry commits.
    /// It waits as a call entering here does, bounded by the leader wait and the attempt budget of the call, and ends at once when the node steps down or
    /// sees a higher term. Any other node refuses at once.
    /// </remarks>
    /// <exception cref="RpcException">The refusal of <see cref="RefuseInternal" /> when this node does not get authority within the wait.</exception>
    private async Task<TResponse> AwaitAuthorityAsync<TState, TResponse>(
        string groupId,
        LeaderRoute route,
        ServerDeadlineBudget call,
        TState state,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken)
    {
        return await _table.WaitForAuthorityAsync(groupId, LeaderWait(in call), cancellationToken).ConfigureAwait(false)
            ? await local(state, cancellationToken).ConfigureAwait(false)
            : throw RefuseInternal(groupId, in route);
    }

    private bool IsSelf(in LeaderRoute route) => string.Equals(route.NodeId, _ownershipResolver.SelfNodeId, StringComparison.Ordinal);

    /// <summary>Refuses a trusted internal owner RPC that reached a node without authority over the key's group; it is never forwarded again.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="route">The leader this node knows; with a static table, always the ring owner.</param>
    /// <returns>
    /// With a static table the stale-owner refusal naming the owner, as before leader routing; otherwise the leader refusal of the group view, with
    /// the leader hint trailers.
    /// </returns>
    private RpcException RefuseInternal(string groupId, in LeaderRoute route)
    {
        var self = _ownershipResolver.SelfNodeId;
        if (_table is StaticLeaderTable)
            return StaleOwnerFailure.Create(route.NodeId, self);

        var view = _table.Read(groupId);
        return LeaderRefusal.Create(LeaderRefusal.Classify(in view, self), in view, self, true);
    }

    /// <summary>Resolves the route of a group without waiting.</summary>
    /// <param name="groupId">The group, named by the ring owner of the key.</param>
    /// <returns>
    /// The known leader; for a group this node does not serve, the leader learned for it, else the ring owner in term zero; otherwise
    /// <see langword="default" />.
    /// </returns>
    /// <remarks>
    /// A group this node holds no election state for goes to the leader a member named, or to its ring owner, a member that names the leader
    /// when it does not lead. The table reports <see langword="default" /> when it knows no leader.
    /// </remarks>
    private LeaderRoute ResolveNow(string groupId)
    {
        var known = _table.TryGetLeader(groupId, out var route) || _table.Read(groupId).Served;
        if (!known && !_table.TryGetLearnedLeader(groupId, out route))
            route = new LeaderRoute(groupId, 0);

        return route;
    }

    /// <summary>Picks the target of the single reroute after a forward could not reach a node of a group this node does not serve, or it had no leader.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="unreachable">The route that could not be reached or had no leader.</param>
    /// <param name="fromTable">Whether the table reported the target: it is the leader learned for the group.</param>
    /// <returns>
    /// The leader learned for the group when it names another node, else the next member after the unreachable one in slot order;
    /// <see langword="default" /> when the group has no other member.
    /// </returns>
    private LeaderRoute PickFallback(string groupId, in LeaderRoute unreachable, out bool fromTable)
    {
        fromTable = _table.TryGetLearnedLeader(groupId, out var learned) && !string.Equals(learned.NodeId, unreachable.NodeId, StringComparison.Ordinal);
        if (fromTable)
            return learned;

        // Only a failed connect or a member without a leader reaches here, so the replica set is resolved off the common path.
        var members = new string[_replicaGroups.ReplicaCount];
        _replicaGroups.GetReplicaGroup(groupId, members);
        var at = Array.IndexOf(members, unreachable.NodeId);
        for (var i = 1; i <= members.Length; i++)
        {
            var next = new LeaderRoute(members[(at + i) % members.Length], 0);
            if (!string.Equals(next.NodeId, unreachable.NodeId, StringComparison.Ordinal) && !IsSelf(in next))
                return next;
        }

        return default;
    }

    /// <summary>Forgets a route that could not be reached, or that had no leader, and picks the target of the single reroute.</summary>
    /// <param name="groupId">The group, which this node does not serve.</param>
    /// <param name="unreachable">The route no attempt of the forward connected to, or that answered it has no leader.</param>
    /// <param name="budget">The shared budget of the operation, created when its first attempt failed.</param>
    /// <param name="next">The target of the reroute and whether the table reported it.</param>
    /// <returns><see langword="true" /> when the reroute was taken; <see langword="false" /> when it was already spent, the time to refusal passed, or no other member exists.</returns>
    private bool TryFallBack(
        string groupId,
        in LeaderRoute unreachable,
        RerouteBudget budget,
        out (LeaderRoute Route, bool FromTable) next)
    {
        // A learned leader that is down or no longer leads is forgotten, so the next call does not try it first.
        _table.Refute(groupId, in unreachable);
        next.Route = PickFallback(groupId, in unreachable, out next.FromTable);
        return !string.IsNullOrEmpty(next.Route.NodeId) && budget.TryConsumeReroute() && !budget.HasExpired();
    }

    /// <summary>Resolves the route of a group, waiting for a leader within the time to refusal and the leader wait when none is known.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="call">The attempt budget of the call.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The route.</returns>
    /// <exception cref="RpcException"><see cref="StatusCode.Unavailable" />: no leader became known within the wait.</exception>
    private async ValueTask<LeaderRoute> ResolveAsync(string groupId, ServerDeadlineBudget call, CancellationToken cancellationToken)
    {
        var route = ResolveNow(groupId);
        var leader = !string.IsNullOrEmpty(route.NodeId) ||
                     (await _table.WaitForLeaderAsync(groupId, LeaderWait(in call), cancellationToken).ConfigureAwait(false) && _table.TryGetLeader(groupId, out route));
        return leader ? route : throw ServerOpContract.NoLeaderAuthority();
    }

    /// <summary>Starts the attempt budget of a call: the ambient budget, which the caller deadline or the forward of the entry node set, capped at one attempt.</summary>
    /// <returns>The budget, counted from the arrival of the call; without a deadline, a new one that starts now.</returns>
    private ServerDeadlineBudget StartCall() => ServerRpcDeadlineContext.CurrentBudget?.CappedAt(_attemptBudget) ??
                                               new ServerDeadlineBudget(_clock, _attemptBudget, _clock.GetTimestamp());

    /// <summary>Bounds a wait for a leader, or for this node's own authority, by the leader wait and the time left in the attempt budget of the call.</summary>
    /// <param name="call">The attempt budget of the call.</param>
    /// <returns>The longest wait; zero once the time to refusal passed.</returns>
    private TimeSpan LeaderWait(in ServerDeadlineBudget call)
    {
        var left = TimeToRefusal(in call);
        var wait = left < _leaderWait ? left : _leaderWait;
        return wait < TimeSpan.Zero ? TimeSpan.Zero : wait;
    }

    /// <summary>Tells whether the leader a refusing node named may take the reroute.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="refused">The route that answered as stale.</param>
    /// <param name="hint">The named leader.</param>
    /// <returns>
    /// <see langword="true" /> for another node of the group's replica set, neither this node (its own authority comes from its table) nor
    /// of a term below the refused route.
    /// </returns>
    private bool IsUsableHint(string groupId, in LeaderRoute refused, in LeaderRoute hint)
    {
        if (string.IsNullOrEmpty(hint.NodeId) || IsSelf(in hint) || string.Equals(hint.NodeId, refused.NodeId, StringComparison.Ordinal) ||
            (hint.Term != 0 && hint.Term < refused.Term))
            return false;

        // Only a stale answer reaches here, so the replica set is resolved off the common path.
        var members = new string[_replicaGroups.ReplicaCount];
        _replicaGroups.GetReplicaGroup(groupId, members);
        return Array.IndexOf(members, hint.NodeId) >= 0;
    }

    /// <summary>Refutes a route that answered as stale and picks the route of the single reroute.</summary>
    /// <param name="groupId">The group.</param>
    /// <param name="refused">The route that answered as stale.</param>
    /// <param name="refusedFromTable">Whether the table reported the refused route; a route taken from a hint is never refuted.</param>
    /// <param name="hint">The leader the refusing node named; <see langword="default" /> when none.</param>
    /// <param name="call">The attempt budget of the call.</param>
    /// <param name="budget">The budget of the operation.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The next route, never <paramref name="refused" />, and whether the table reported it.</returns>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.Unavailable" />: the reroute was already spent, the time to refusal passed, no other route is known, or no leader became
    /// known within the wait.
    /// </exception>
    private async ValueTask<(LeaderRoute Route, bool FromTable)> RerouteAsync(
        string groupId,
        LeaderRoute refused,
        bool refusedFromTable,
        LeaderRoute hint,
        ServerDeadlineBudget call,
        RerouteBudget budget,
        CancellationToken cancellationToken)
    {
        // A refutation hides every known leader of its term or below, so only a route the table reported is refuted, never a peer-supplied term.
        if (refusedFromTable)
            _table.Refute(groupId, in refused);

        // A usable hint is learned before the budget is checked, so the next call of a group this node does not serve goes to that leader.
        var usable = IsUsableHint(groupId, in refused, in hint);
        if (usable)
            _table.Learn(groupId, in hint);

        if (!budget.TryConsumeReroute() || budget.HasExpired())
            throw ServerOpContract.LeaderChanged();

        if (usable)
            return (hint, false);

        var next = await ResolveAsync(groupId, call, cancellationToken).ConfigureAwait(false);
        return string.Equals(next.NodeId, refused.NodeId, StringComparison.Ordinal) ? throw ServerOpContract.LeaderChanged() : (next, true);
    }

    private RerouteBudget BudgetFor(in ServerDeadlineBudget call, ref RerouteBudget? budget) =>
        budget ??= RerouteBudget.FromRemaining(TimeToRefusal(in call), _clock);

    private async ValueTask<(LeaderRoute Route, bool FromTable, bool FellBack)> RecoverRouteAsync(
        string groupId,
        LeaderRoute previous,
        ServerDeadlineBudget call,
        (RerouteBudget Budget, bool FailedConnect) recovery,
        CancellationToken cancellationToken)
    {
        if (_table.Read(groupId).Served)
        {
            if (!recovery.FailedConnect || !recovery.Budget.TryConsumeReroute() || TimeToRefusal(in call) <= TimeSpan.Zero)
                return default;

            var route = await _table.WaitForLeaderChangeAsync(groupId, previous, LeaderWait(in call), cancellationToken).ConfigureAwait(false);
            return TimeToRefusal(in call) > TimeSpan.Zero ? (route, true, false) : default;
        }

        return TryFallBack(groupId, in previous, recovery.Budget, out var next) ? (next.Route, next.FromTable, true) : default;
    }

    /// <summary>Runs the call on the leader of a group, rerouting once when the route answers as stale.</summary>
    /// <typeparam name="TState">The state the call delegates read.</typeparam>
    /// <typeparam name="TResponse">The response type.</typeparam>
    /// <param name="groupId">The group.</param>
    /// <param name="known">The route resolved without waiting; <see langword="default" /> when none is known yet.</param>
    /// <param name="call">The attempt budget of the call.</param>
    /// <param name="state">The call state.</param>
    /// <param name="forward">Forwards the call.</param>
    /// <param name="local">Runs the call on this node.</param>
    /// <param name="cancellationToken">The call cancellation token.</param>
    /// <returns>The response of the attempt that answered.</returns>
    /// <remarks>
    /// The budget, created at the first stale or unreachable answer, allows one reroute, so at most two logical attempts run: a third is
    /// refused. The forward runs under the attempt budget of the call, so it carries that budget as its deadline. A member the fallback reached that serves the call is learned as the leader of the group.
    /// </remarks>
    private async Task<TResponse> RouteAsync<TState, TResponse>(
        string groupId,
        LeaderRoute known,
        ServerDeadlineBudget call,
        TState state,
        Func<TState, string, CancellationToken, Task<TResponse>> forward,
        Func<TState, CancellationToken, Task<TResponse>> local,
        CancellationToken cancellationToken)
    {
        var route = string.IsNullOrEmpty(known.NodeId) ? await ResolveAsync(groupId, call, cancellationToken).ConfigureAwait(false) : known;
        var fromTable = true;
        var fellBack = false;
        RerouteBudget? budget = null;
        while (true)
        {
            try
            {
                TResponse response;
                using (IsSelf(in route) ? null : ServerRpcDeadlineContext.Push(in call))
                    response = await AttemptAsync(in route, state, forward, local, cancellationToken).ConfigureAwait(false);

                if (fellBack)
                    _table.Learn(groupId, in route);

                return response;
            }
            catch (RpcException ex) when (StaleRouteSignals.TryReadStale(ex, out var hint))
            {
                var shared = BudgetFor(in call, ref budget);
                (route, fromTable) = await RerouteAsync(groupId, route, fromTable, hint, call, shared, cancellationToken).ConfigureAwait(false);
                fellBack = false;
            }
            catch (RpcException ex) when (OwnerUnreachableFailure.IsLocal(ex) || IsNoLeader(ex))
            {
                var shared = BudgetFor(in call, ref budget);
                var next = await RecoverRouteAsync(groupId, route, call, (shared, OwnerUnreachableFailure.IsLocal(ex)), cancellationToken).ConfigureAwait(false);
                if (string.IsNullOrEmpty(next.Route.NodeId))
                    throw;

                (route, fromTable, fellBack) = next;
            }
        }
    }
}
