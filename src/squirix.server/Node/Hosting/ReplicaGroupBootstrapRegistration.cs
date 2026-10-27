using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;

namespace Squirix.Server.Node.Hosting;

/// <summary>Registers the first start of the replica group this node owns.</summary>
internal static class ReplicaGroupBootstrapRegistration
{
    /// <summary>Registers the service that seeds the group this node owns once every other member can be seeded with it.</summary>
    /// <param name="services">DI service collection.</param>
    /// <remarks>Registered whenever the replica group registry exists, with or without automatic failover.</remarks>
    internal static void Add(IServiceCollection services) =>
        _ = services.AddHostedService(static sp => new ReplicaGroupBootstrapService(
            Create(sp),
            (
                sp.GetRequiredService<ReplicaGroupRegistry>().PeerContact,
                sp.GetRequiredService<IHostApplicationLifetime>(),
                sp.GetRequiredService<ReplicaGroupCommitters>(),
                sp.GetRequiredService<ReplicaGroupRegistry>().ApplySignalFor(sp.GetRequiredService<TopologyOptions>().NodeId)),
            sp.GetRequiredService<ILogger<ReplicaGroupBootstrapService>>(),
            sp.GetRequiredService<TimeProvider>()));

    /// <summary>Creates the first start of the group this node owns.</summary>
    /// <param name="sp">The service provider.</param>
    /// <returns>The first start.</returns>
    /// <exception cref="InvalidOperationException">This node serves no log of the group it owns.</exception>
    private static ReplicaGroupBootstrap Create(IServiceProvider sp)
    {
        var topology = sp.GetRequiredService<TopologyOptions>();
        var locator = sp.GetRequiredService<IReplicaGroupLocator>();
        if (!sp.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(topology.NodeId, out var log))
            throw new InvalidOperationException($"This node does not serve the replica group '{topology.NodeId}' it owns.");

        var members = new string[locator.ReplicaCount];
        locator.GetReplicaGroup(topology.NodeId, members);
        byte[] fingerprint = [.. TopologyFingerprint.CreateFromTopology(topology, sp.GetRequiredService<MtlsOptions>()).Bytes];
        var header = new ReplicaRpcHeader(
            topology.NodeId,
            fingerprint,
            topology.ConfigurationGeneration,
            0,
            string.Empty,
            topology.NodeId);
        return new ReplicaGroupBootstrap(log, sp.GetRequiredService<IReplicaBootstrapGateway>(), members, in header, ReplicaVerificationProbe.ProbeTimeout);
    }
}
