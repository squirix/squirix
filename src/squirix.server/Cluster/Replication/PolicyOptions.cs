namespace Squirix.Server.Cluster.Replication;

/// <summary>Static replication policy constants that participate in topology identity.</summary>
internal static class PolicyOptions
{
    // MaxReplicaCount is owned by Cluster.TopologyConstraints; Replication aliases it (child → parent).

    /// <summary>Canonical fingerprint format version for preview.8 topology identity.</summary>
    internal const int CanonicalFormatVersion = 1;

    /// <summary>Closed replication message payload size limit in bytes (fingerprint input).</summary>
    internal const int ClosedMessageMaxBytes = 16 * 1024 * 1024;

    /// <summary>Closed replica snapshot size limit in bytes (fingerprint input).</summary>
    internal const int ClosedSnapshotMaxBytes = 64 * 1024 * 1024;

    /// <summary>Durability schema version included in the topology fingerprint.</summary>
    internal const int DurabilitySchemaVersion = 1;

    /// <summary>Consistent-hash algorithm version for vnode ownership.</summary>
    internal const int HashAlgorithmVersion = 1;

    /// <summary>Maximum supported replica factor for preview.8.</summary>
    internal const int MaxReplicaCount = TopologyConstraints.MaxReplicaCount;

    /// <summary>Minimum cluster package version required for RF&gt;1 topology agreement.</summary>
    internal const string MinClusterPackageVersion = "0.1.0-preview.8";

    /// <summary>Physical replica placement algorithm version.</summary>
    internal const int PlacementAlgorithmVersion = 1;

    /// <summary>Private replication protocol algorithm version.</summary>
    internal const int ProtocolAlgorithmVersion = 1;

    /// <summary>Quorum acknowledgement mode: majority without lease (fingerprint token).</summary>
    internal const string QuorumAckMode = "majority-no-lease";

    /// <summary>Default RF&gt;1 idempotency capacity included in the fingerprint.</summary>
    internal const int RfIdempotencyMaxInFlightRecords = 65_536;

    /// <summary>Default RF&gt;1 idempotency retention ticks included in the fingerprint.</summary>
    internal const long RfIdempotencyRetentionTicks = 15L * 60L * 10_000_000L;
}
