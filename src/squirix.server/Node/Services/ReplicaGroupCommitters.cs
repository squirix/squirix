using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Errors;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>The committers of the replica groups this node leads, fixed for the node lifetime.</summary>
/// <remarks>
/// A write reaches the committer of the group that owns its key: the group named by the original owner of the key. A key of a group this
/// node does not lead is refused as a stale owner, so nothing of it is appended. While the node leads only its own group, every key that
/// reaches this set is owned by it (the ownership guard above refuses the others), so the owner is not looked up again.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaGroupCommitters : IAsyncDisposable
{
    private readonly FrozenDictionary<string, ReplicaGroupCommitter> _byGroup;
    private readonly ReplicaGroupCommitter[] _led;
    private readonly INodeLocator _owners;

    /// <summary>The committer of the own group when it is the only group this node leads; otherwise <see langword="null" />.</summary>
    private readonly ReplicaGroupCommitter? _ownOnly;

    private readonly string _selfId;
    private int _disposed;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupCommitters" /> class.</summary>
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

        _led = committers;
        _byGroup = byGroup.ToFrozenDictionary(StringComparer.Ordinal);
        _ownOnly = committers.Length == 1 && string.Equals(committers[0].GroupId, selfId, StringComparison.Ordinal) ? committers[0] : null;
        _selfId = selfId;
        _owners = owners;
        Clock = clock;
    }

    /// <summary>Gets the leader clock that decides expiry and times the expiration sweep.</summary>
    internal TimeProvider Clock { get; }

    /// <summary>Gets the committers of the led groups, in registry order.</summary>
    internal IReadOnlyList<ReplicaGroupCommitter> Led => _led;

    /// <inheritdoc />
    /// <remarks>
    /// The committers are disposed concurrently, each within its own shutdown budget, so a committer stuck in its drain neither delays nor
    /// hides the disposal of the others. Every committer is disposed even when another fails, and such a failure surfaces only after all
    /// of them finished.
    /// </remarks>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
            return;

        var disposals = new Task[_led.Length];
        for (var i = 0; i < _led.Length; i++)
            disposals[i] = _led[i].DisposeAsync().AsTask();

        await Task.WhenAll(disposals).ConfigureAwait(false);
    }

    /// <summary>Gets the committer of a led group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns>The committer.</returns>
    /// <exception cref="KeyNotFoundException">This node does not lead the group.</exception>
    internal ReplicaGroupCommitter For(string groupId) => _byGroup[groupId];

    /// <summary>Gets the committer of the group that owns a key.</summary>
    /// <param name="cacheName">Canonical cache name of the operation.</param>
    /// <param name="key">User key of the operation.</param>
    /// <returns>The committer of the owning group.</returns>
    /// <exception cref="Grpc.Core.RpcException">This node does not lead the owning group: the stale-owner refusal, nothing was appended.</exception>
    internal ReplicaGroupCommitter ForKey(string cacheName, string key)
    {
        if (_ownOnly is { } own)
            return own;

        var owner = _owners.GetOwner(cacheName, key);
        return _byGroup.TryGetValue(owner, out var committer) ? committer : throw StaleOwnerFailure.Create(owner, _selfId);
    }

    /// <summary>Tells whether this node leads a group.</summary>
    /// <param name="groupId">Replica group identifier.</param>
    /// <returns><see langword="true" /> when this node leads the group.</returns>
    internal bool Leads(string groupId) => _byGroup.ContainsKey(groupId);
}
