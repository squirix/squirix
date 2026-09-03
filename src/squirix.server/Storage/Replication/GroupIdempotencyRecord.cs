using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Durable idempotency record for one replica-group operation.</summary>
/// <param name="OperationScope">The scope of the operation identifier (client operation or internal expiration).</param>
/// <param name="OperationId">The stable logical mutation identifier.</param>
/// <param name="OperationFingerprint">Canonical bytes of the exact request that produced the outcome.</param>
/// <param name="OutcomePayload">The exact resolved outcome bytes, or an empty payload while unresolved.</param>
/// <param name="Kind">The record kind.</param>
/// <param name="CreatedUtc">When the reservation was created through the injected time source.</param>
/// <param name="ResolvedUtc">When the outcome was resolved, or <see langword="null" /> while the record is unresolved.</param>
/// <param name="LogIndex">The journal index that carried the record.</param>
/// <param name="Term">The term in which the record was appended.</param>
/// <remarks>
///     <para>
///     Synthesized equality and hash-code members compare the <see cref="ReadOnlyMemory{T}" /> fields by
///     underlying-object reference, index, and length — not by content, so two records holding identical
///     fingerprint or payload bytes in different arrays are not equal.
///     </para>
///     <para>
///     Do not use this type as a dictionary key or rely on structural equality across codec round-trips;
///     compare payload bytes explicitly via <c language="csharp">SequenceEqual</c>-style span helpers.
///     </para>
/// </remarks>
[Immutable]
internal readonly record struct GroupIdempotencyRecord(
    string OperationScope,
    string OperationId,
    ReadOnlyMemory<byte> OperationFingerprint,
    ReadOnlyMemory<byte> OutcomePayload,
    GroupRecordKind Kind,
    DateTime CreatedUtc,
    DateTime? ResolvedUtc,
    ulong LogIndex,
    ulong Term)
{
    /// <summary>Gets a value indicating whether this record is still unresolved.</summary>
    internal bool IsUnresolved => ResolvedUtc == null;

    /// <summary>Gets a value indicating whether this record has a durable resolved outcome.</summary>
    internal bool IsResolved => ResolvedUtc != null;

    /// <summary>Returns a copy of this record with the resolved outcome and timestamp applied.</summary>
    /// <param name="outcomePayload">The exact resolved outcome bytes.</param>
    /// <param name="resolvedUtc">The resolution timestamp.</param>
    /// <returns>A resolved copy of this record.</returns>
    internal GroupIdempotencyRecord Resolve(ReadOnlyMemory<byte> outcomePayload, DateTime resolvedUtc) => this with
    {
        OutcomePayload = outcomePayload,
        ResolvedUtc = resolvedUtc,
    };
}
