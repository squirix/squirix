using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical domain form of one replicated log entry, twin of the transport ReplicaLogEntry.</summary>
/// <param name="LogIndex">The one-based log index of the entry.</param>
/// <param name="Term">The term in which the leader created the entry.</param>
/// <param name="OperationId">Client operation identifier for idempotent retries.</param>
/// <param name="OperationScope">Operation scope distinguishing user mutations from expirations.</param>
/// <param name="OperationFingerprint">Operation identity fingerprint.</param>
/// <param name="RecordKind">Wire record kind carried opaquely for the follower applier.</param>
/// <param name="CacheName">Target cache name.</param>
/// <param name="KeyPayload">Target key bytes.</param>
/// <param name="MutationKind">Cache mutation kind carried opaquely for the follower applier.</param>
/// <param name="MutationPayload">Cache mutation bytes.</param>
/// <param name="OutcomePayload">Committed outcome bytes observed by the caller.</param>
/// <param name="ExpiresUtcTicks">Mutation-specific expiration wire value: a <see cref="TimeSpan" /> duration in ticks for the Touch mutation kind, an absolute UTC timestamp in ticks for all other mutation kinds.</param>
/// <param name="CreatedUtcTicks">Creation time expressed as UTC ticks.</param>
/// <param name="ResolvedUtcTicks">Resolution time expressed as UTC ticks.</param>
/// <param name="PayloadChecksum">Wire payload checksum carried opaquely.</param>
/// <remarks>
/// The transport message stays in the adapter layer; this record carries the same fields in domain types.
/// <see cref="ReplicaLogCodec" /> encodes it to the canonical bytes stored in follower logs, so owner,
/// follower log, and follower applier observe identical bytes.
/// </remarks>
[Immutable]
internal readonly record struct ReplicaLogRecord(
    ulong LogIndex,
    ulong Term,
    string OperationId,
    string OperationScope,
    ReadOnlyMemory<byte> OperationFingerprint,
    string RecordKind,
    string CacheName,
    ReadOnlyMemory<byte> KeyPayload,
    string MutationKind,
    ReadOnlyMemory<byte> MutationPayload,
    ReadOnlyMemory<byte> OutcomePayload,
    long ExpiresUtcTicks,
    long CreatedUtcTicks,
    long ResolvedUtcTicks,
    uint PayloadChecksum);
