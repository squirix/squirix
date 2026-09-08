using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Node.Replication;
using Squirix.Server.Storage;
using Squirix.Server.Utils;

namespace Squirix.Server.TestKit.Replication;

/// <summary>Offline RF=1 to RF&gt;1 bootstrap preparation for stopped-node scenarios.</summary>
public static class OfflineBootstrapTestKit
{
    /// <summary>Prepares a bootstrap manifest with an explicit target topology and reports the seeded groups.</summary>
    /// <param name="dataDirectory">Stopped node data directory.</param>
    /// <param name="groupIds">Replica groups to seed.</param>
    /// <param name="targetReplicaCount">Seeded target replica count.</param>
    /// <param name="targetGeneration">Seeded target configuration generation.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>The prepared manifest summary.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="dataDirectory" /> or <paramref name="groupIds" /> is <see langword="null" />.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when the target replica count or generation is invalid.</exception>
    public static Task<OfflineBootstrapSummary> PrepareAsync(
        string dataDirectory,
        IReadOnlyList<string> groupIds,
        int targetReplicaCount,
        ulong targetGeneration,
        CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(dataDirectory);
        ArgumentNullException.ThrowIfNull(groupIds);
        if (targetReplicaCount <= 1)
            throw new ArgumentOutOfRangeException(nameof(targetReplicaCount), targetReplicaCount, "Bootstrap target must replicate (RF > 1).");

        if (targetGeneration == 0)
            throw new ArgumentOutOfRangeException(nameof(targetGeneration), targetGeneration, "Bootstrap target generation must be positive.");

        return PrepareCoreAsync(dataDirectory, groupIds, targetReplicaCount, targetGeneration, cancellationToken);
    }

    private static async Task<OfflineBootstrapSummary> PrepareCoreAsync(
        string dataDirectory,
        IReadOnlyList<string> groupIds,
        int targetReplicaCount,
        ulong targetGeneration,
        CancellationToken cancellationToken)
    {
        var request = new BootstrapPreparationRequest
        {
            GroupIds = groupIds,
            LegacyOutcomes = [],
            Persistence = new PersistenceOptions { DataDir = dataDirectory },
            SourceMtls = new MtlsOptions { InternalListenPort = 7000 },
            SourceTopology = Topology(1, 1UL),
            TargetMtls = new MtlsOptions { InternalListenPort = 7000 },
            TargetTopology = Topology(targetReplicaCount, targetGeneration),
        };

        var prepared = await new BootstrapPlanner().PrepareAsync(request, cancellationToken).ConfigureAwait(false);
        var decoded = await new BootstrapManifestStore(dataDirectory).ReadAsync(cancellationToken).ConfigureAwait(false) ??
                      ThrowHelper.Throw<BootstrapManifest>(new InvalidOperationException($"Bootstrap manifest is missing in '{dataDirectory}' after preparation."));

        var pending = new List<string>(decoded.Groups.Count);
        foreach (var group in decoded.Groups)
            pending.Add($"{group.GroupId}:{group.State}");

        return new OfflineBootstrapSummary(decoded.TargetReplicaCount, decoded.TargetGeneration, pending, prepared.Resumed);
    }

    private static TopologyOptions Topology(int replicaCount, ulong generation)
    {
        var peers = new[]
        {
            new ServerPeer
            {
                InterNodeUri = new Uri("https://127.0.0.1:7001"),
                NodeId = "node-a",
                Uri = new Uri("https://127.0.0.1:6001"),
            },
            new ServerPeer
            {
                InterNodeUri = new Uri("https://127.0.0.1:7002"),
                NodeId = "node-b",
                Uri = new Uri("https://127.0.0.1:6002"),
            },
            new ServerPeer
            {
                InterNodeUri = new Uri("https://127.0.0.1:7003"),
                NodeId = "node-c",
                Uri = new Uri("https://127.0.0.1:6003"),
            },
        };
        return new TopologyOptions(peers)
        {
            ClusterId = "cluster-a",
            ConfigurationGeneration = generation,
            NodeId = "node-a",
            ReplicaCount = replicaCount,
            Uri = peers[0].Uri,
            VirtualNodes = 128,
        };
    }
}
