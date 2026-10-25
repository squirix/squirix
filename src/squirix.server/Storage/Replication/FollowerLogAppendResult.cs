using System;
using Squirix.Server.Attributes;

namespace Squirix.Server.Storage.Replication;

/// <summary>Outcome of a replica log append attempt.</summary>
/// <param name="Success">Determines whether the appending was accepted.</param>
/// <param name="RefusalCode">Stable refusal marker when the appending was not accepted; otherwise empty.</param>
/// <param name="CurrentTerm">The durable term after processing the request.</param>
/// <param name="LastLogIndex">The durable last log index after processing the request.</param>
[Immutable]
internal readonly record struct FollowerLogAppendResult(bool Success, string RefusalCode, ulong CurrentTerm, ulong LastLogIndex)
{
    /// <summary>Gets a value indicating whether the replying node reported critical memory pressure; an older node reports <see langword="false" />.</summary>
    internal bool MemoryPressure { get; init; }

    /// <summary>Tells whether the reply shows that the sender is the current leader of the copy.</summary>
    /// <param name="requestTerm">The term of the append request.</param>
    /// <returns>
    /// <see langword="true" /> for an accepted append, a log mismatch, and an incomplete copy that refuses a request whose term is not stale: the
    /// leader only lacks entries or waits for a state transfer.
    /// </returns>
    internal bool ShowsLeader(ulong requestTerm) =>
        Success ||
        string.Equals(RefusalCode, FollowerLogRefusal.LogMismatch, StringComparison.Ordinal) ||
        (string.Equals(RefusalCode, FollowerLogRefusal.Incomplete, StringComparison.Ordinal) && CurrentTerm <= requestTerm);
}
