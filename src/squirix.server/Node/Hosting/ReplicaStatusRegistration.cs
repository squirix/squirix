using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Node.Hosting;

/// <summary>Registers the status of the served replica groups and the readiness check that reads it.</summary>
internal static class ReplicaStatusRegistration
{
    /// <summary>Registers the replica status source and the replica readiness health check.</summary>
    /// <param name="services">Node component registrations.</param>
    internal static void Add(NodeServices services)
    {
        _ = services.Own<IReplicaStatusSource>(static sp => new ReplicaGroupStatusSource(
            sp.Require<ReplicaGroupRegistry>(),
            sp.Require<TopologyOptions>(),
            sp.Require<MtlsOptions>(),
            sp.Require<IReplicaGroupLocator>(),
            sp.Require<ReplicaGroupCommitters>().CoordinatorFor,
            sp.Require<TopologyOptions>().NodeId));
        _ = services.Use(new ReadinessCheck(
            "replica_readiness",
            static sp => new ReplicaReadinessHealthCheck(sp.Require<IReplicaStatusSource>(), sp.Require<ReplicationMetrics>())));
    }
}
