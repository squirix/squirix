using System;
using System.Collections.Generic;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Cluster.Membership;
using Squirix.Server.Runtime.Contracts;

namespace Squirix.Server.Node.Cluster;

/// <summary>
/// Static cluster topology and ring diagnostics for admin REST endpoints.
/// </summary>
internal sealed class AdminClusterDiagnosticsService : IAdminClusterDiagnostics
{
    private readonly ClusterConfig _cluster;
    private readonly INodeLocator _locator;

    public AdminClusterDiagnosticsService(ClusterConfig cluster, INodeLocator locator)
    {
        _cluster = cluster ?? throw new ArgumentNullException(nameof(cluster));
        _locator = locator ?? throw new ArgumentNullException(nameof(locator));
    }

    /// <inheritdoc />
    public AdminMembersDiagnosticsSnapshot GetMembersDiagnostics() => new()
    {
        Members = GetNodeIds(_cluster),
        VirtualNodes = _cluster.VirtualNodes,
    };

    /// <inheritdoc />
    public AdminRebalanceHistorySnapshot GetRebalanceHistory() => new() { Retention = 0, Events = [] };

    /// <inheritdoc />
    public AdminRingDiagnosticsSnapshot GetRingDiagnostics(int sampleSize)
    {
        var members = GetNodeIds(_cluster);
        Array.Sort(members, StringComparer.Ordinal);

        var distribution = new Dictionary<string, int>(members.Length, StringComparer.Ordinal);
        foreach (var member in members)
            distribution[member] = 0;

        const int ownerLookupPreviewCount = 16;
        var ownerLookupPreview = new List<AdminRingOwnerSample>(ownerLookupPreviewCount);

        for (var i = 0; i < sampleSize; i++)
        {
            var key = $"ring-sample-{i:0000}";
            var owner = _locator.GetOwner(CacheNames.DefaultNamespace, key);
            _ = distribution.TryGetValue(owner, out var currentCount);
            distribution[owner] = currentCount + 1;

            if (i < ownerLookupPreviewCount)
                ownerLookupPreview.Add(new AdminRingOwnerSample(key, owner));
        }

        var ownerIds = new string[distribution.Count];
        distribution.Keys.CopyTo(ownerIds, 0);
        Array.Sort(ownerIds, StringComparer.Ordinal);

        var objects = new List<AdminRingNodeDistributionSnapshot>(ownerIds.Length);
        foreach (var ownerId in ownerIds)
        {
            var sampleCount = distribution[ownerId];
            objects.Add(new AdminRingNodeDistributionSnapshot(ownerId, sampleCount, Math.Round((double)sampleCount / sampleSize, 4), _cluster.VirtualNodes));
        }

        return new AdminRingDiagnosticsSnapshot
        {
            VirtualNodes = _cluster.VirtualNodes,
            Members = members,
            SampleSize = sampleSize,
            VnodeDistribution = objects,
            OwnerLookupSamples = ownerLookupPreview,
        };
    }

    private static string[] GetNodeIds(ClusterConfig cluster)
    {
        var peers = cluster.Peers;
        var nodeIds = new string[peers.Length];
        for (var i = 0; i < peers.Length; i++)
            nodeIds[i] = peers[i].NodeId;
        return nodeIds;
    }
}
