using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Closed policy vector participating in the canonical topology fingerprint.</summary>
/// <remarks>
/// Values are captured per fingerprint computation, so policy drift between nodes invalidates
/// agreement. Production vectors come from <see cref="Default" />; tests derive variants with
/// <c language="csharp">with</c> expressions.
/// </remarks>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct FingerprintPolicy
{
    /// <summary>Gets the production policy vector sourced from <see cref="PolicyOptions" />.</summary>
    internal static FingerprintPolicy Default { get; } = new()
    {
        CanonicalFormatVersion = PolicyOptions.CanonicalFormatVersion,
        ClosedMessageMaxBytes = PolicyOptions.ClosedMessageMaxBytes,
        ClosedSnapshotMaxBytes = PolicyOptions.ClosedSnapshotMaxBytes,
        DurabilitySchemaVersion = PolicyOptions.DurabilitySchemaVersion,
        HashAlgorithmVersion = PolicyOptions.HashAlgorithmVersion,
        MaxReplicaCount = PolicyOptions.MaxReplicaCount,
        PlacementAlgorithmVersion = PolicyOptions.PlacementAlgorithmVersion,
        ProtocolAlgorithmVersion = PolicyOptions.ProtocolAlgorithmVersion,
        RfIdempotencyMaxInFlightRecords = PolicyOptions.RfIdempotencyMaxInFlightRecords,
        RfIdempotencyRetentionTicks = PolicyOptions.RfIdempotencyRetentionTicks,
    };

    /// <summary>Gets the canonical fingerprint format version.</summary>
    internal required int CanonicalFormatVersion { get; init; }

    /// <summary>Gets the closed replication message size limit.</summary>
    internal required int ClosedMessageMaxBytes { get; init; }

    /// <summary>Gets the closed replica snapshot size limit.</summary>
    internal required int ClosedSnapshotMaxBytes { get; init; }

    /// <summary>Gets the durability schema version.</summary>
    internal required int DurabilitySchemaVersion { get; init; }

    /// <summary>Gets the hash algorithm version.</summary>
    internal required int HashAlgorithmVersion { get; init; }

    /// <summary>Gets the protocol maximum replica count.</summary>
    internal required int MaxReplicaCount { get; init; }

    /// <summary>Gets the placement algorithm version.</summary>
    internal required int PlacementAlgorithmVersion { get; init; }

    /// <summary>Gets the private protocol algorithm version.</summary>
    internal required int ProtocolAlgorithmVersion { get; init; }

    /// <summary>Gets the RF&gt;1 idempotency capacity.</summary>
    internal required int RfIdempotencyMaxInFlightRecords { get; init; }

    /// <summary>Gets the RF&gt;1 idempotency retention in ticks.</summary>
    internal required long RfIdempotencyRetentionTicks { get; init; }
}
