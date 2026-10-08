using System;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Runtime.Invocation;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Builds owner routers over an unfenced ring agreement.</summary>
internal static class OwnerRouters
{
    /// <summary>The leader wait the routers use unless a test sets its own.</summary>
    internal static readonly TimeSpan LeaderWait = TimeSpan.FromSeconds(2);

    /// <summary>Creates a router over the static leader table of <paramref name="self" />, as on a node no election leads.</summary>
    /// <param name="ownership">Resolves the ring owner of a key.</param>
    /// <param name="invocation">Tells whether the call is a trusted internal owner RPC.</param>
    /// <param name="self">This node.</param>
    /// <returns>The router.</returns>
    internal static OwnerRouter Static(INodeOwnershipResolver ownership, IRemoteInvocationState invocation, string self) =>
        new(ownership, invocation, RingAgreements.Create(), new StaticLeaderTable(self), LeaderWait, TimeProvider.System, Locator(self));

    /// <summary>Creates a replica group locator whose every group holds every given node.</summary>
    /// <param name="nodes">The nodes of the ring; at most five.</param>
    /// <returns>The locator.</returns>
    internal static IReplicaGroupLocator Locator(params string[] nodes) => new ReplicaGroupLocator(new PhysicalNodeRing(nodes), nodes.Length);
}
