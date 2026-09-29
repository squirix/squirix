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
/// <param name="MutationPayload">
/// The entry the record writes, in the durable entry encoding that keeps its version and tags, when the effect is an upsert; empty
/// when the effect is a delete or leaves memory unchanged.
/// </param>
/// <param name="OutcomePayload">
/// The outcome the leader decided at prepare time: the applied flag and, for a remove, the removed entry. It is the one outcome the
/// client, the group idempotency state and every recovery of the record report, and the effect of the record must agree with it.
/// </param>
/// <param name="ExpiresUtcTicks">
/// The absolute UTC deadline in ticks of the entry an upserting record writes, pinned by the leader at prepare time and applied
/// verbatim; zero when the written entry never expires and for records that write nothing.
/// </param>
/// <param name="DecidedUtcTicks">The leader time of the decision, in UTC ticks, for diagnostics only: no applier reads it.</param>
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
    long DecidedUtcTicks,
    long ResolvedUtcTicks,
    uint PayloadChecksum);
