using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Ordinal-sorted physical node ring used to select the next RF−1 distinct followers for an original owner.</summary>
internal sealed class PhysicalNodeRing
{
    private readonly string[] _nodes;

    /// <summary>Initializes a new instance of the <see cref="PhysicalNodeRing" /> class.</summary>
    /// <param name="nodeIds">Configured peer node identifiers.</param>
    /// <exception cref="ArgumentException">Thrown when no distinct node identifiers are provided.</exception>
    internal PhysicalNodeRing(ReadOnlySpan<string> nodeIds)
    {
        _nodes = CollectSortedDistinct(nodeIds);
        if (_nodes.Length is 0)
            throw new ArgumentException("At least one node must be provided.", nameof(nodeIds));
    }

    /// <summary>Gets the number of distinct physical nodes on the ring.</summary>
    internal int Count => _nodes.Length;

    /// <summary>
    /// Writes the ordered replica group for <paramref name="originalOwnerNodeId" /> into <paramref name="destination" />.
    /// Destination length must equal <paramref name="replicaCount" />. Index 0 is the original owner.
    /// </summary>
    /// <param name="originalOwnerNodeId">Original owner selected by the vnode ring.</param>
    /// <param name="replicaCount">Replica factor including the original owner.</param>
    /// <param name="destination">Caller-owned destination of length <paramref name="replicaCount" />.</param>
    /// <exception cref="ArgumentException">Thrown when the owner is unknown or destination length is invalid.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="replicaCount" /> is out of range.</exception>
    internal void WriteReplicaGroup(string originalOwnerNodeId, int replicaCount, Span<string> destination)
    {
        var ownerIndex = ValidateAndResolveOwner(originalOwnerNodeId, replicaCount, destination);
        destination[0] = _nodes[ownerIndex];
        for (var i = 1; i < replicaCount; i++)
            destination[i] = _nodes[(ownerIndex + i) % _nodes.Length];
    }

    private static string[] CollectSortedDistinct(ReadOnlySpan<string> nodeIds)
    {
        var distinct = DistinctNodeIds.InInsertionOrder(nodeIds);
        if (distinct.Length > 1)
            Array.Sort(distinct, StringComparer.Ordinal);

        return distinct;
    }

    private int IndexOf(string nodeId) => Array.BinarySearch(_nodes, nodeId, StringComparer.Ordinal);

    private int ValidateAndResolveOwner(string originalOwnerNodeId, int replicaCount, Span<string> destination)
    {
        ArgumentException.ThrowIfNullOrEmpty(originalOwnerNodeId);
        ValidateReplicaCount(replicaCount);
        if (destination.Length != replicaCount)
            throw new ArgumentException("Destination length must equal replicaCount.", nameof(destination));

        var ownerIndex = IndexOf(originalOwnerNodeId);
        if (ownerIndex < 0)
            throw new ArgumentException("Original owner is not present on the physical ring.", nameof(originalOwnerNodeId));

        return ownerIndex;
    }

    private void ValidateReplicaCount(int replicaCount)
    {
        if (replicaCount < 1 || replicaCount > PolicyOptions.MaxReplicaCount || replicaCount > _nodes.Length)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count is out of range for the physical ring.");
    }
}
