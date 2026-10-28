using System;
using Squirix.Server.Storage.Replication;
using Squirix.Transport.Grpc;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Static replication policy constants that participate in topology identity.</summary>
internal static class PolicyOptions
{
    /// <summary>Canonical fingerprint format version; version 2 hashes the automatic failover and quorum read switches.</summary>
    internal const int CanonicalFormatVersion = 2;

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

    /// <summary>Minimum cluster package version required for RF>1 topology agreement.</summary>
    internal const string MinClusterPackageVersion = "0.1.0-preview.10";

    /// <summary>Physical replica placement algorithm version.</summary>
    internal const int PlacementAlgorithmVersion = 1;

    /// <summary>Private replication protocol algorithm version.</summary>
    internal const int ProtocolAlgorithmVersion = 1;

    /// <summary>Quorum acknowledgement mode: majority without lease (fingerprint token).</summary>
    internal const string QuorumAckMode = "majority-no-lease";

    /// <summary>Default RF>1 idempotency capacity included in the fingerprint.</summary>
    internal const int RfIdempotencyMaxInFlightRecords = 1 << 19;

    /// <summary>Default RF>1 bound on the bytes the idempotency outcomes are charged, included in the fingerprint.</summary>
    /// <remarks>The snapshot limit less one closed replication message; see <see cref="GroupIdempotencyState.DefaultMaxOutcomeBytes" />.</remarks>
    internal const long RfIdempotencyMaxOutcomeBytes = GroupIdempotencyState.DefaultMaxOutcomeBytes;

    /// <summary>The difference between the wall clocks of two nodes that group idempotency tolerates; operators keep node clocks within it.</summary>
    /// <remarks>An outcome is aged on a node from the decision time the leader stamped, so a node clock that runs ahead by this much still keeps the outcome for the retry window the SDK needs.</remarks>
    internal static readonly TimeSpan RfIdempotencyClockSkewAllowance = TimeSpan.FromSeconds(10);

    /// <summary>The time after its decision an outcome is always replayable: twice the SDK operation deadline.</summary>
    /// <remarks>
    /// A retry of the SDK arrives within one operation deadline of its first attempt. The window adds the clock skew allowance and the time
    /// a request spends in transit and in the commit budget, so a retry after a change of leader, or on a node whose clock runs ahead, still
    /// finds its outcome.
    /// </remarks>
    internal static readonly TimeSpan RfIdempotencyRetryWindow = RpcAttemptTimeout.OperationDeadline * 2;

    /// <summary>Default RF>1 idempotency retention ticks included in the fingerprint: the retry window, with no longer soft retention.</summary>
    internal static readonly long RfIdempotencyRetentionTicks = RfIdempotencyRetryWindow.Ticks;

    /// <summary>Default RF>1 minimum idempotency retention ticks (an outcome is always replayable this long) included in the fingerprint.</summary>
    internal static readonly long RfIdempotencyMinRetentionTicks = RfIdempotencyRetryWindow.Ticks;
}
