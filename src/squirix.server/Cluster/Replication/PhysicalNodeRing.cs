using System;
using System.Collections.Generic;

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
        ArgumentException.ThrowIfNullOrEmpty(originalOwnerNodeId);
        if (replicaCount < 1 || replicaCount > PolicyOptions.MaxReplicaCount || replicaCount > _nodes.Length)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count is out of range for the physical ring.");

        if (destination.Length != replicaCount)
            throw new ArgumentException("Destination length must equal replicaCount.", nameof(destination));

        var ownerIndex = IndexOf(originalOwnerNodeId);
        if (ownerIndex < 0)
            throw new ArgumentException("Original owner is not present on the physical ring.", nameof(originalOwnerNodeId));

        destination[0] = _nodes[ownerIndex];
        for (var i = 1; i < replicaCount; i++)
            destination[i] = _nodes[(ownerIndex + i) % _nodes.Length];
    }

    private static string[] CollectSortedDistinct(ReadOnlySpan<string> nodeIds)
    {
        var seen = new HashSet<string>(StringComparer.Ordinal);
        var buffer = new string[nodeIds.Length];
        var write = 0;
        for (var i = 0; i < nodeIds.Length; i++)
        {
            var value = nodeIds[i];
            if (string.IsNullOrWhiteSpace(value) || !seen.Add(value))
                continue;

            buffer[write++] = value;
        }

        if (write is 0)
            return [];

        if (write == buffer.Length)
        {
            Array.Sort(buffer, StringComparer.Ordinal);
            return buffer;
        }

        var result = new string[write];
        for (var i = 0; i < write; i++)
            result[i] = buffer[i];

        Array.Sort(result, StringComparer.Ordinal);
        return result;
    }

    private int IndexOf(string nodeId)
    {
        for (var i = 0; i < _nodes.Length; i++)
        {
            if (string.Equals(_nodes[i], nodeId, StringComparison.Ordinal))
                return i;
        }

        return -1;
    }
}
