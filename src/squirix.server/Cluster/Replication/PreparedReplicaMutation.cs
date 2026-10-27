using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Immutable canonical state for one mutation before any durable append starts.</summary>
[Immutable]
internal sealed record PreparedReplicaMutation
{
    /// <summary>Initializes a new instance of the <see cref="PreparedReplicaMutation" /> class.</summary>
    /// <param name="identity">Canonical operation identity.</param>
    /// <param name="term">Replica term that owns the mutation.</param>
    /// <param name="logIndex">Reserved group log index.</param>
    /// <param name="payload">Canonical payloads.</param>
    internal PreparedReplicaMutation(ReplicaOperationIdentity identity, ulong term, ulong logIndex, ReplicaMutationPayload payload)
        : this(identity, term, logIndex, payload, true)
    {
    }

    private PreparedReplicaMutation(ReplicaOperationIdentity identity, ulong term, ulong logIndex, ReplicaMutationPayload payload, bool copyBuffers)
    {
        ArgumentNullException.ThrowIfNull(identity);
        ArgumentNullException.ThrowIfNull(payload);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.GroupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.OperationId);
        ArgumentException.ThrowIfNullOrWhiteSpace(identity.OperationScope);
        if (term == 0)
            throw new ArgumentOutOfRangeException(nameof(term), "Replica term must be positive.");

        if (logIndex == 0)
            throw new ArgumentOutOfRangeException(nameof(logIndex), "Replica log index must be positive.");

        GroupId = identity.GroupId;
        Term = term;
        LogIndex = logIndex;
        OperationId = identity.OperationId;
        OperationScope = identity.OperationScope;
        OperationFingerprint = copyBuffers ? identity.OperationFingerprint.ToArray() : identity.OperationFingerprint;
        CanonicalPayload = copyBuffers ? payload.CanonicalPayload.ToArray() : payload.CanonicalPayload;
        OutcomePayload = copyBuffers ? payload.OutcomePayload.ToArray() : payload.OutcomePayload;
        PayloadChecksum = payload.PayloadChecksum;
    }

    internal ReadOnlyMemory<byte> CanonicalPayload { get; }

    internal string GroupId { get; }

    internal ulong LogIndex { get; }

    internal ReadOnlyMemory<byte> OperationFingerprint { get; }

    internal string OperationId { get; }

    internal string OperationScope { get; }

    internal ReadOnlyMemory<byte> OutcomePayload { get; }

    internal uint PayloadChecksum { get; }

    internal ulong Term { get; }

    /// <summary>Creates a mutation that stores the given buffers without copying them.</summary>
    /// <param name="identity">Canonical operation identity.</param>
    /// <param name="term">Replica term that owns the mutation.</param>
    /// <param name="logIndex">Reserved group log index.</param>
    /// <param name="payload">Canonical payloads.</param>
    /// <returns>The mutation, which owns the fingerprint, canonical and outcome buffers.</returns>
    /// <remarks>The caller hands the buffers over: nobody else holds them, they are not pooled, and nobody mutates them afterwards.</remarks>
    internal static PreparedReplicaMutation TakeOwnership(ReplicaOperationIdentity identity, ulong term, ulong logIndex, ReplicaMutationPayload payload) =>
        new(identity, term, logIndex, payload, false);
}
