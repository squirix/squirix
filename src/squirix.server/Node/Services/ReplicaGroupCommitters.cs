using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Errors;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The committers of the replica groups this node leads: fixed for the node lifetime, or handed over by the election.</summary>
/// <remarks>
/// <para>
/// A write reaches the committer of the group that owns its key: the group named by the original owner of the key. A key of a group this
/// node does not lead is refused as a stale owner, so nothing of it is appended. While the node leads only its own group statically, every
/// key that reaches this set is owned by it (the ownership guard above refuses the others), so the owner is not looked up again.
/// </para>
/// <para>
/// With automatic failover the set starts empty and is the leadership the election drivers hand won terms to. The committer of a group
/// is created on its first promotion and kept for the node lifetime; a promotion adds it to the led groups and a retirement removes it,
/// each as a new snapshot. A write is admitted only while the leader table reports local authority for its group, so a leader whose
/// leader-term entry is not committed yet, or that a higher term deposed, refuses it like a stale owner.
/// </para>
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaGroupCommitters : IReplicaLeadership, IAsyncDisposable
{
    private readonly IGroupLeaderTable? _authority;
    private readonly Func<string, ReplicaGroupCommitter>? _create;
    private readonly Dictionary<string, ReplicaGroupCommitter> _created = [with(StringComparer.Ordinal)];
    private readonly INodeLocator _owners;

    /// <summary>The committer of the own group when it is the only group this node leads statically; otherwise <see langword="null" />.</summary>
    private readonly ReplicaGroupCommitter? _ownOnly;

    private readonly Channel<ReplicaPromotion>? _promotions;
    private readonly string _selfId;
    private readonly Lock _sync = new();
    private int _disposed;
    private LedGroups _led;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitters" /> class that leads a fixed set of groups.</summary>
    /// <param name="led">The committers of the led groups, in registry order; each group at most once.</param>
    /// <param name="selfId">Identifier of this node, which owns the group with the same identifier.</param>
    /// <param name="owners">The key owner lookup that names the group of a key.</param>
    /// <param name="clock">The leader clock that decides expiry and times the expiration sweep.</param>
    /// <exception cref="ArgumentException">A group is led twice, or the node identifier is missing.</exception>
    internal ReplicaGroupCommitters(IReadOnlyList<ReplicaGroupCommitter> led, string selfId, INodeLocator owners, TimeProvider clock)
    {
        ArgumentNullException.ThrowIfNull(led);
        ArgumentException.ThrowIfNullOrWhiteSpace(selfId);
        ArgumentNullException.ThrowIfNull(owners);
        ArgumentNullException.ThrowIfNull(clock);
        var committers = new ReplicaGroupCommitter[led.Count];
        var byGroup = new Dictionary<string, ReplicaGroupCommitter>(led.Count, StringComparer.Ordinal);
        for (var i = 0; i < committers.Length; i++)
        {
            var committer = led[i] ?? ThrowHelper.Throw<ReplicaGroupCommitter>(new ArgumentException("A led group has no committer.", nameof(led)));
            if (!byGroup.TryAdd(committer.GroupId, committer))
                throw new ArgumentException($"The replica group '{committer.GroupId}' is led twice.", nameof(led));

            committers[i] = committer;
        }

        _led = new LedGroups(byGroup.ToFrozenDictionary(StringComparer.Ordinal), committers);
        _ownOnly = committers.Length == 1 && string.Equals(committers[0].GroupId, selfId, StringComparison.Ordinal) ? committers[0] : null;
        _selfId = selfId;
        _owners = owners;
        Clock = clock;
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitters" /> class that leads the groups the election hands it.</summary>
    /// <param name="create">Creates the committer of a served group, which leads it by election; called once per group.</param>
    /// <param name="authority">The leader table that tells whether this node has authority in a group.</param>
    /// <param name="selfId">Identifier of this node.</param>
    /// <param name="owners">The key owner lookup that names the group of a key.</param>
    /// <param name="clock">The leader clock that decides expiry and times the expiration sweep.</param>
    internal ReplicaGroupCommitters(Func<string, ReplicaGroupCommitter> create, IGroupLeaderTable authority, string selfId, INodeLocator owners, TimeProvider clock)
        : this([], selfId, owners, clock)
    {
        ArgumentNullException.ThrowIfNull(create);
        ArgumentNullException.ThrowIfNull(authority);
        _create = create;
        _authority = authority;
        _promotions = Channel.CreateUnbounded<ReplicaPromotion>(new UnboundedChannelOptions { SingleReader = true });
    }

    /// <summary>Gets the leader clock that decides expiry and times the expiration sweep.</summary>
    internal TimeProvider Clock { get; }

    /// <summary>Gets the committers of the led groups: a snapshot, which a promotion or a retirement replaces.</summary>
    internal IReadOnlyList<ReplicaGroupCommitter> Led => Volatile.Read(ref _led).Committers;

    /// <summary>Gets the leaderships the election starts, in order, for the readiness service; <see langword="null" /> when the led set is fixed.</summary>
    internal ChannelReader<ReplicaPromotion>? Promotions => _promotions?.Reader;

    /// <inheritdoc />
    /// <remarks>
    /// Every committer created is disposed, led or not, concurrently, each within its own shutdown budget, so a committer stuck in its
    /// drain neither delays nor hides the disposal of the others. Every committer is disposed even when another fails, and such a failure
    /// surfaces only after all of them finished.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        _ = _promotions?.Writer.TryComplete();
        ReplicaGroupCommitter[] committers;
        lock (_sync)
            committers = _create == null ? Volatile.Read(ref _led).Committers : [.. _created.Values];

        var disposals = new Task[committers.Length];
        for (var i = 0; i < committers.Length; i++)
            disposals[i] = committers[i].DisposeAsync().AsTask();

        await Task.WhenAll(disposals).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public Task HeartbeatAsync(string groupId, CancellationToken cancellationToken)
    {
        Created(groupId)?.RunningPipeline?.Heartbeat();
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    /// <exception cref="InvalidOperationException">The led set is fixed, or the group still leads another term.</exception>
    public async Task<bool> PromoteAsync(string groupId, ulong term, CancellationToken cancellationToken)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
        var committer = GetOrCreate(groupId);
        var started = committer.Tenure != null;
        var authorized = await committer.PromoteAsync(term, cancellationToken).ConfigureAwait(false);
        if (!started && committer.Tenure is { } tenure && _promotions != null)
        {
            Publish(committer, true);
            _ = _promotions.Writer.TryWrite(new ReplicaPromotion(committer, tenure.Token));
        }

        return authorized;
    }

    /// <inheritdoc />
    public async Task<bool> RetireAsync(string groupId, CancellationToken cancellationToken)
    {
        if (Created(groupId) is not { } committer)
            return true;

        var retired = await committer.RetireAsync(cancellationToken).ConfigureAwait(false);
        if (retired)
            Publish(committer, false);

        return retired;
    }

    /// <summary>Finds the committer of a group this node may write to: a led group, with local authority when the election leads it.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The committer, or <see langword="null" /> when this node does not lead the group or has no authority in it.</returns>
    internal ReplicaGroupCommitter? FindAuthorized(string groupId) =>
        Volatile.Read(ref _led).ByGroup.GetValueOrDefault(groupId) is { } committer && _authority?.HasLocalAuthority(groupId, out _) != false ? committer : null;

    /// <summary>Gets the committer of a led group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The committer.</returns>
    /// <exception cref="KeyNotFoundException">This node does not lead the group.</exception>
    internal ReplicaGroupCommitter For(string groupId) => Volatile.Read(ref _led).ByGroup[groupId];

    /// <summary>Gets the committer of the group that owns a key.</summary>
    /// <param name="cacheName">Canonical cache name of the operation.</param>
    /// <param name="key">User key of the operation.</param>
    /// <returns>The committer of the owning group.</returns>
    /// <exception cref="Grpc.Core.RpcException">
    /// This node does not lead the owning group, or has no authority in it: the stale-owner refusal, naming the known leader of the group
    /// when there is one; nothing was appended.
    /// </exception>
    internal ReplicaGroupCommitter ForKey(string cacheName, string key)
    {
        if (_ownOnly is { } own)
            return own;

        var owner = _owners.GetOwner(cacheName, key);
        return FindAuthorized(owner) ?? ThrowHelper.Throw<ReplicaGroupCommitter>(StaleOwnerFailure.Create(LeaderOf(owner), _selfId));
    }

    /// <summary>Tells whether this node leads a group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns><see langword="true" /> when this node leads the group.</returns>
    internal bool Leads(string groupId) => Volatile.Read(ref _led).ByGroup.ContainsKey(groupId);

    private ReplicaGroupCommitter? Created(string groupId)
    {
        lock (_sync)
            return _created.GetValueOrDefault(groupId);
    }

    private ReplicaGroupCommitter GetOrCreate(string groupId)
    {
        var create = ThrowHelper.Required(_create, "The led groups of this node are fixed; none is promoted.");
        lock (_sync)
        {
            if (!_created.TryGetValue(groupId, out var committer))
            {
                committer = create(groupId);
                _created.Add(groupId, committer);
            }

            return committer;
        }
    }

    /// <summary>Names the node a refused write is pointed to: the known leader of the group when it is another node, otherwise its owner.</summary>
    /// <param name="groupId">Replica group identifier, the original owner of the key.</param>
    /// <returns>The node identifier.</returns>
    private string LeaderOf(string groupId) =>
        _authority is { } table && table.TryGetLeader(groupId, out var route) && !string.Equals(route.NodeId, _selfId, StringComparison.Ordinal) ? route.NodeId : groupId;

    /// <summary>Adds a committer to the led groups or removes it, as a new snapshot.</summary>
    /// <param name="committer">The committer.</param>
    /// <param name="led">Whether the group is led from now on.</param>
    private void Publish(ReplicaGroupCommitter committer, bool led)
    {
        lock (_sync)
        {
            var current = Volatile.Read(ref _led);
            var byGroup = new Dictionary<string, ReplicaGroupCommitter>(current.ByGroup, StringComparer.Ordinal);
            if (led)
                byGroup[committer.GroupId] = committer;
            else
                _ = byGroup.Remove(committer.GroupId);

            var committers = new ReplicaGroupCommitter[byGroup.Count];
            byGroup.Values.CopyTo(committers, 0);
            Volatile.Write(ref _led, new LedGroups(byGroup.ToFrozenDictionary(StringComparer.Ordinal), committers));
        }
    }

    /// <summary>One snapshot of the led groups.</summary>
    /// <param name="ByGroup">The committers by group identifier.</param>
    /// <param name="Committers">The committers.</param>
    [Immutable]
    private sealed record LedGroups(FrozenDictionary<string, ReplicaGroupCommitter> ByGroup, ReplicaGroupCommitter[] Committers);
}
