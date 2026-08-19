using System.Collections.Generic;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Inputs required to compute a canonical topology fingerprint.</summary>
[Immutable]
internal sealed class FingerprintInputs
{
    /// <summary>Initializes a new instance of the <see cref="FingerprintInputs" /> class.</summary>
    internal FingerprintInputs()
    {
        ClusterId = "cluster";
        ConfigurationGeneration = 1;
        MinClusterPackageVersion = PolicyOptions.MinClusterPackageVersion;
        Peers = [];
        QuorumAckMode = PolicyOptions.QuorumAckMode;
        ReplicaCount = 1;
        VirtualNodes = 128;
        RfIdempotencyMaxInFlightRecords = PolicyOptions.RfIdempotencyMaxInFlightRecords;
        RfIdempotencyRetentionTicks = PolicyOptions.RfIdempotencyRetentionTicks;
        ClosedMessageMaxBytes = PolicyOptions.ClosedMessageMaxBytes;
        ClosedSnapshotMaxBytes = PolicyOptions.ClosedSnapshotMaxBytes;
    }

    /// <summary>Gets the canonical fingerprint format version.</summary>
    internal int CanonicalFormatVersion { get; } = PolicyOptions.CanonicalFormatVersion;

    /// <summary>Gets the closed replication message size limit.</summary>
    internal int ClosedMessageMaxBytes { get; }

    /// <summary>Gets the closed replica snapshot size limit.</summary>
    internal int ClosedSnapshotMaxBytes { get; }

    /// <summary>Gets the cluster identifier.</summary>
    internal required string ClusterId { get; init; }

    /// <summary>Gets the stopped-topology configuration generation.</summary>
    internal required ulong ConfigurationGeneration { get; init; }

    /// <summary>Gets the durability schema version.</summary>
    internal int DurabilitySchemaVersion { get; } = PolicyOptions.DurabilitySchemaVersion;

    /// <summary>Gets the hash algorithm version.</summary>
    internal int HashAlgorithmVersion { get; } = PolicyOptions.HashAlgorithmVersion;

    /// <summary>Gets the protocol maximum replica count.</summary>
    internal int MaxReplicaCount { get; } = PolicyOptions.MaxReplicaCount;

    /// <summary>Gets the minimum cluster package version token.</summary>
    internal required string MinClusterPackageVersion { get; init; }

    /// <summary>Gets peer descriptors included in the fingerprint.</summary>
    internal required IReadOnlyList<FingerprintPeer> Peers { get; init; }

    /// <summary>Gets the placement algorithm version.</summary>
    internal int PlacementAlgorithmVersion { get; } = PolicyOptions.PlacementAlgorithmVersion;

    /// <summary>Gets the private protocol algorithm version.</summary>
    internal int ProtocolAlgorithmVersion { get; init; } = PolicyOptions.ProtocolAlgorithmVersion;

    /// <summary>Gets the quorum acknowledgement mode token.</summary>
    internal required string QuorumAckMode { get; init; }

    /// <summary>Gets the configured replica factor.</summary>
    internal required int ReplicaCount { get; init; }

    /// <summary>Gets RF&gt;1 idempotency capacity.</summary>
    internal int RfIdempotencyMaxInFlightRecords { get; init; }

    /// <summary>Gets RF&gt;1 idempotency retention in ticks.</summary>
    internal long RfIdempotencyRetentionTicks { get; }

    /// <summary>Gets the vnode count used by the ownership ring.</summary>
    internal required int VirtualNodes { get; init; }
}
