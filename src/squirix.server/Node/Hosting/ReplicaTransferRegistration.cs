using System;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Composition;
using Squirix.Server.LocalCache;
using Squirix.Server.Logging;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Node.Hosting;

/// <summary>Registers the receiver of the state transfer of a replica group, the port through which it reaches the cache of the node, and the sender of the leader.</summary>
internal static class ReplicaTransferRegistration
{
    /// <summary>Registers the state sink and the transfer receiver.</summary>
    /// <param name="services">Node component registrations.</param>
    /// <remarks>The container owns the receiver and disposes its idle timers on host shutdown.</remarks>
    internal static void Add(NodeServices services)
    {
        _ = services.Own<IReplicaStateSink>(static sp => new ReplicaStateSink(
            sp.Require<ReplicaGroupAppliers>(),
            sp.Require<ReplicaGroupCommitters>(),
            sp.Require<OwnerLocalCache>().Value,
            new ReplicaGroupEntries(sp.Require<ILocalCacheSnapshotReader<object?>>(), sp.Require<INodeLocator>()),
            sp.Require<IJournalCoordinator>(),
            sp.Require<ISnapshotReadinessStatus>()));
        _ = services.Own(static sp => new ReplicaTransferReceiver(
            sp.Require<ReplicaGroupRegistry>(),
            sp.Require<IReplicaMembership>(),
            sp.Require<IReplicaStateSink>(),
            sp.Find<ReplicaTransferOptions>() ?? new ReplicaTransferOptions(),
            sp.Require<TimeProvider>(),
            sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaTransferReceiver>(),
            sp.Require<TopologyOptions>().NodeId));
        _ = services.Own(static sp => new ReplicaStateTransfers(
            sp.Require<IReplicaTransferGateway>(),
            new ReplicaGroupEntries(sp.Require<ILocalCacheSnapshotReader<object?>>(), sp.Require<INodeLocator>()),
            sp.Require<IJournalCoordinator>(),
            sp.Find<ReplicaTransferOptions>() ?? new ReplicaTransferOptions(),
            sp.Require<IServerLoggerFactory>().CreateLogger<ReplicaStateTransfers>(),
            sp.Require<ReplicaCatchUpMetrics>())
        {
            ChecksumRetryOptions = sp.Find<ReplicaReadinessOptions>() ?? new ReplicaReadinessOptions(),
            TermCeiling = new ReplicaTermCeiling(sp.Require<IReplicaBootstrapGateway>(), ReplicaVerificationProbe.ProbeTimeout),
        });
    }
}
