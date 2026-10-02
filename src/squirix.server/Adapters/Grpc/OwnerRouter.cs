using System;
using Grpc.Core;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Decides, before any idempotency or pipeline work, whether an inbound single-key RPC runs on this node or is forwarded to the key owner.</summary>
[Immutable]
internal sealed class OwnerRouter
{
    private readonly IRemoteInvocationState _invocationState;
    private readonly INodeOwnershipResolver _ownershipResolver;

    internal OwnerRouter(INodeOwnershipResolver ownershipResolver, IRemoteInvocationState invocationState)
    {
        ArgumentNullException.ThrowIfNull(ownershipResolver);
        ArgumentNullException.ThrowIfNull(invocationState);
        _ownershipResolver = ownershipResolver;
        _invocationState = invocationState;
    }

    /// <summary>Finds the node a client call must be forwarded to.</summary>
    /// <param name="cacheName">The cache name from the request.</param>
    /// <param name="key">The key from the request.</param>
    /// <returns>The owner node id when another node owns the key; <see langword="null" /> when the call runs on this node.</returns>
    /// <remarks>An invalid cache name or key runs on this node, so the canonical validation error is raised as for any local call.</remarks>
    /// <exception cref="RpcException">
    /// <see cref="StatusCode.FailedPrecondition" /> with the stale-owner trailer when a trusted internal owner RPC reaches a node that does not own the key.
    /// </exception>
    internal string? FindRemoteOwner(string cacheName, string key)
    {
        if (!ServerCacheName.TryParsePublic(cacheName, out var canonicalName) || !CacheKeyValidator.TryValidate(key, out _))
            return null;

        var owner = _ownershipResolver.GetOwner(canonicalName, key);
        var isLocal = string.Equals(owner, _ownershipResolver.SelfNodeId, StringComparison.Ordinal);
        return (isLocal, _invocationState.IsInternalOwnerInvocation) switch
        {
            (true, _) => null,
            (false, false) => owner,
            (false, true) => throw StaleOwnerFailure.Create(owner, _ownershipResolver.SelfNodeId),
        };
    }
}
