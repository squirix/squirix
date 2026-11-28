using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.App.Decorators;

/// <summary>Refuses operations on keys of groups this node does not lead with authority before anything commits, journals or takes a key gate.</summary>
/// <remarks>
/// <para>
/// The group of a key is named by its ring owner. Without elections the owner leads its group statically, so a key another node owns is
/// refused with the same stale-owner failure a trusted internal call gets, and nothing else changes.
/// </para>
/// <para>
/// When elections lead the groups, a key passes only while this node leads its group with authority. Otherwise the refusal comes from the
/// leader view: stale-term when a higher term deposed this node, stale-owner naming the known leader in the hint trailers, or the retryable
/// Unavailable refusal while no leader is known. A group this node does not serve is refused as a stale owner naming the ring owner, which
/// serves it. Reads and writes are guarded alike.
/// </para>
/// <para>
/// The guard refuses before the group log is consulted, so a retry of an operation whose entry a deposed node appended but never resolved
/// gets a stale marker here rather than the unknown outcome. That is safe: a reroute keeps the operation id, and the current leader answers
/// it from the group log: it replays a committed entry, reports an unresolved one as unknown, and runs the operation once when the entry
/// was truncated.
/// </para>
/// </remarks>
/// <typeparam name="T">The cache value type.</typeparam>
[Immutable]
internal sealed class OwnershipGuardCacheDecorator<T> : IRequestCache<T>
{
    private readonly ILogicalNamespacedCache<T> _inner;
    private readonly bool _leaderHints;
    private readonly IGroupLeaderTable _leaders;
    private readonly INodeLocator _locator;
    private readonly string _self;
    private readonly IRoutedKeyContext? _routing;

    /// <summary>Initializes a new instance of the <see cref="OwnershipGuardCacheDecorator{T}" /> class.</summary>
    /// <param name="self">This node.</param>
    /// <param name="locator">The ring owner lookup that names the group of a key.</param>
    /// <param name="leaders">The leader table of the groups.</param>
    /// <param name="leaderHints">Whether elections lead the groups, so a stale-owner refusal names the elected leader in the hint trailers.</param>
    /// <param name="inner">The next cache in the pipeline.</param>
    /// <param name="routing">The group already found for a matching inbound request.</param>
    internal OwnershipGuardCacheDecorator(string self, INodeLocator locator, IGroupLeaderTable leaders, bool leaderHints, ILogicalNamespacedCache<T> inner, IRoutedKeyContext? routing = null)
    {
        ArgumentNullException.ThrowIfNull(self);
        ArgumentNullException.ThrowIfNull(locator);
        ArgumentNullException.ThrowIfNull(leaders);
        ArgumentNullException.ThrowIfNull(inner);
        _self = self;
        _locator = locator;
        _leaders = leaders;
        _leaderHints = leaderHints;
        _inner = inner;
        _routing = routing;
    }

    public ValueTask<NodeCacheEntry<T>?> GetEntryAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.GetEntryForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<NodeCacheValueRead<T>> GetValueWithDeadlineAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.GetValueWithDeadlineForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<NodeCacheValueResult<T>> GetValueAsync(string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.GetValueForRequestAsync(cacheName, key, cancellationToken, request);
    }

    public ValueTask<bool> RemoveAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.RemoveForRequestAsync(operationId, cacheName, key, cancellationToken, request);
    }

    public ValueTask<bool> RemoveExpirationAsync(string operationId, string cacheName, string key, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.RemoveExpirationForRequestAsync(operationId, cacheName, key, cancellationToken, request);
    }

    public ValueTask SetEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.SetEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> TouchAsync(string operationId, string cacheName, string key, TimeSpan expiration, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.TouchForRequestAsync(operationId, cacheName, key, expiration, cancellationToken, request);
    }

    public ValueTask<bool> TryAddEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.TryAddEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> TryReplaceEntryAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.TryReplaceEntryForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<NodeCacheAddOrGetResult<T>> AddOrGetAsync(string operationId, string cacheName, string key, NodeCacheEntry<T> entry, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.AddOrGetForRequestAsync(operationId, cacheName, key, entry, cancellationToken, request);
    }

    public ValueTask<bool> UpdateAsync(string operationId, string cacheName, string key, T? value, ICacheRequestContext? request, CancellationToken cancellationToken)
    {
        EnsureLocalOwner(cacheName, key, request);
        return _inner.UpdateForRequestAsync(operationId, cacheName, key, value, cancellationToken, request);
    }

    private void EnsureLocalOwner(string cacheName, string key, IRoutedKeyContext? request)
    {
        var owner = (request ?? _routing)?.FindGroup(cacheName, key) ?? _locator.GetOwner(cacheName, key);
        var view = _leaders.Read(owner);
        if (!view.Served)
            throw StaleOwnerFailure.Create(owner, _self);

        var kind = LeaderRefusal.Classify(in view, _self);
        if (kind != LeaderRefusalKind.None)
            throw LeaderRefusal.Create(kind, in view, _self, _leaderHints);
    }
}
