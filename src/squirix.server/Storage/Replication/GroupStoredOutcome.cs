using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>A retained group idempotency record with the monotonic anchor its retention is counted from.</summary>
/// <param name="Record">The record.</param>
/// <param name="AnchorTimestamp">The monotonic timestamp the age is counted from; unused while the record is unresolved.</param>
/// <param name="AgeAtAnchor">The age the record already had at <paramref name="AnchorTimestamp" />, carried over from a snapshot.</param>
/// <param name="Sequence">The generation of a resolved record in the expiry order; <c language="csharp">0</c> while the record is unresolved.</param>
/// <param name="Bytes">The bytes the record is charged against the byte bound, fixed when it is stored.</param>
[Immutable]
internal readonly record struct GroupStoredOutcome(GroupIdempotencyRecord Record, long AnchorTimestamp, TimeSpan AgeAtAnchor, long Sequence, long Bytes)
{
    /// <summary>Returns how long ago the record was resolved, on the monotonic clock.</summary>
    /// <param name="clock">The clock the anchor was read from.</param>
    /// <returns>The age the record was restored with plus the time since it was anchored.</returns>
    internal TimeSpan Age(TimeProvider clock) => AgeAtAnchor + clock.GetElapsedTime(AnchorTimestamp);

    /// <summary>Moves an unresolved record to the coordinates it is re-reserved at; a resolved record keeps its own.</summary>
    /// <param name="logIndex">The journal index that now carries the record.</param>
    /// <param name="term">The term in which the record was appended again.</param>
    /// <param name="moved">The record at the new coordinates when it moved; otherwise this record.</param>
    /// <returns><see langword="true" /> when the record is unresolved and its coordinates differ.</returns>
    internal bool TryMoveTo(ulong logIndex, ulong term, out GroupStoredOutcome moved)
    {
        var move = Record.IsUnresolved && (Record.LogIndex != logIndex || Record.Term != term);
        moved = move ? this with { Record = Record with { LogIndex = logIndex, Term = term } } : this;
        return move;
    }

    /// <summary>Returns the record to export, its resolution time restated as the capture time minus its age.</summary>
    /// <param name="clock">The clock the anchor was read from.</param>
    /// <param name="capturedUtc">The capture time, on the clock the snapshot stamps.</param>
    /// <returns>The record whose resolution time is at its age, rounded up to a whole millisecond, before the capture; it saturates at the earliest date.</returns>
    internal GroupIdempotencyRecord Export(TimeProvider clock, DateTime capturedUtc)
    {
        var age = Age(clock);
        var partial = age.Ticks % TimeSpan.TicksPerMillisecond;
        age = partial == 0 || age > TimeSpan.MaxValue - TimeSpan.FromMilliseconds(1) ? age : age.Add(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - partial));
        var resolvedUtc = age < capturedUtc - DateTime.MinValue ? capturedUtc - age : DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc);
        return Record with { ResolvedUtc = resolvedUtc };
    }
}
