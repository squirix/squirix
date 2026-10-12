using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Hosting;

/// <summary>Registers the receiver of the state transfer of a replica group, the port through which it reaches the cache of the node, and the sender of the leader.</summary>
internal static class ReplicaTransferRegistration
{
    /// <summary>Registers the state sink and the transfer receiver.</summary>
    /// <param name="services">DI service collection.</param>
    /// <remarks>The container owns the receiver and disposes its idle timers on host shutdown.</remarks>
    internal static void Add(IServiceCollection services)
    {
        _ = services.AddSingleton<IReplicaStateSink>(static sp => new ReplicaStateSink(
            sp.GetRequiredService<ReplicaGroupAppliers>(),
            sp.GetRequiredService<ReplicaGroupCommitters>(),
            sp.GetRequiredKeyedService<ILogicalNamespacedCache<object?>>(CachePipelineRegistration.LocalChainKey),
            new ReplicaGroupEntries(sp.GetRequiredService<ILocalCacheSnapshotReader<object?>>(), sp.GetRequiredService<INodeLocator>()),
            sp.GetRequiredService<IJournalCoordinator>(),
            sp.GetRequiredService<IJournalCoordinator>()));
        _ = services.AddSingleton(static sp => new ReplicaTransferReceiver(
            sp.GetRequiredService<ReplicaGroupRegistry>(),
            sp.GetRequiredService<IReplicaMembership>(),
            sp.GetRequiredService<IReplicaStateSink>(),
            sp.GetService<ReplicaTransferOptions>() ?? new ReplicaTransferOptions(),
            sp.GetRequiredService<TimeProvider>(),
            sp.GetRequiredService<ILogger<ReplicaTransferReceiver>>()));
        _ = services.AddSingleton(static sp => new ReplicaStateTransfers(
            sp.GetRequiredService<IReplicaTransferGateway>(),
            new ReplicaGroupEntries(sp.GetRequiredService<ILocalCacheSnapshotReader<object?>>(), sp.GetRequiredService<INodeLocator>()),
            sp.GetRequiredService<IJournalCoordinator>(),
            sp.GetService<ReplicaTransferOptions>() ?? new ReplicaTransferOptions(),
            sp.GetRequiredService<ILogger<ReplicaStateTransfers>>(),
            sp.GetRequiredService<ReplicaCatchUpMetrics>()));
    }
}
