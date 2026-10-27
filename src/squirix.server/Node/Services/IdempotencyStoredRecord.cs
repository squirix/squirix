using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Snapshot;

namespace Squirix.Server.Node.Services;

/// <summary>A record with the monotonic timestamp it was created at in this process, its insertion sequence and the reservation holding it.</summary>
/// <param name="Record">The persisted record.</param>
/// <param name="CreatedTimestamp">The monotonic creation timestamp; <see langword="null" /> for a record restored from disk.</param>
/// <param name="Sequence">The insertion sequence capacity eviction orders by.</param>
/// <param name="Reservation">The completion of the attempt that acquired a Started record; only that attempt settles it.</param>
/// <param name="Appended">The outcome of a Started record whose outcome frame is enqueued but not yet durable; a snapshot exports it.</param>
/// <param name="Stamped">Whether a mutation frame stamped with the operation id was enqueued for this live reservation.</param>
[Immutable]
internal readonly record struct IdempotencyStoredRecord(
    PersistedIdempotencyRecord Record,
    long? CreatedTimestamp,
    long Sequence,
    TaskCompletionSource? Reservation,
    PersistedIdempotencyRecord? Appended,
    bool Stamped = false)
{
    /// <summary>Gets the bytes the record is charged against the byte budget: the overhead, the response, and the outcome held for a snapshot cut.</summary>
    internal long Charge => IdempotencyOptions.RecordOverheadBytes + Record.ResponseBytes.Length + (Appended?.ResponseBytes.Length ?? 0);
}
