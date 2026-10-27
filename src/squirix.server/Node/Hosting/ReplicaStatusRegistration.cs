using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Node.Hosting;

/// <summary>Registers the status of the served replica groups and the readiness check that reads it.</summary>
internal static class ReplicaStatusRegistration
{
    /// <summary>Registers the replica status source and the replica readiness health check.</summary>
    /// <param name="services">DI service collection.</param>
    internal static void Add(IServiceCollection services)
    {
        _ = services.AddSingleton<IReplicaStatusSource>(static sp => new ReplicaGroupStatusSource(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<TopologyOptions>(),
            sp.GetRequiredService<MtlsOptions>(),
            sp.GetRequiredService<IReplicaGroupLocator>(),
            sp.GetRequiredService<ReplicaGroupCommitters>().CoordinatorFor,
            sp.GetRequiredService<TopologyOptions>().NodeId));
        _ = services.AddHealthChecks().Add(
            new HealthCheckRegistration(
                "replica_readiness",
                static sp => new ReplicaReadinessHealthCheck(sp.GetRequiredService<IReplicaStatusSource>(), sp.GetRequiredService<ReplicationMetrics>()),
                HealthStatus.Unhealthy,
                ["ready"]));
    }
}
