using System;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Default <see cref="IReplicaGroupLocator" /> backed by a <see cref="PhysicalNodeRing" />.</summary>
internal sealed class ReplicaGroupLocator : IReplicaGroupLocator
{
    private readonly PhysicalNodeRing _ring;

    /// <summary>Initializes a new instance of the <see cref="ReplicaGroupLocator" /> class.</summary>
    /// <param name="ring">Physical node ring.</param>
    /// <param name="replicaCount">Replica factor including the original owner.</param>
    internal ReplicaGroupLocator(PhysicalNodeRing ring, int replicaCount)
    {
        ArgumentNullException.ThrowIfNull(ring);
        if (replicaCount < 1 || replicaCount > PolicyOptions.MaxReplicaCount || replicaCount > ring.Count)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), replicaCount, "Replica count is out of range for the physical ring.");

        _ring = ring;
        ReplicaCount = replicaCount;
    }

    /// <inheritdoc />
    public int ReplicaCount { get; }

    /// <inheritdoc />
    public void GetReplicaGroup(string originalOwnerNodeId, Span<string> destination) =>
        _ring.WriteReplicaGroup(originalOwnerNodeId, ReplicaCount, destination);
}
