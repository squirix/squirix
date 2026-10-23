using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The answer of the transfer receiver to one message of a state transfer.</summary>
/// <param name="Term">The durable term of the copy when the receiver answered; zero when this node does not serve the group.</param>
/// <param name="Success">Whether the message was accepted.</param>
/// <param name="RefusalCode">The stable refusal marker; empty when the message was accepted.</param>
/// <param name="More">Whether the leader must send the same step again: only a wipe step that has not emptied the group sets it.</param>
[Immutable]
internal readonly record struct ReplicaTransferReply(ulong Term, bool Success, string RefusalCode, bool More)
{
    /// <summary>Creates the reply to an accepted message.</summary>
    /// <param name="term">The durable term of the copy.</param>
    /// <param name="more">Whether the same step must be sent again.</param>
    /// <returns>The reply.</returns>
    internal static ReplicaTransferReply Accepted(ulong term, bool more = false) => new(term, true, string.Empty, more);

    /// <summary>Creates the reply to a refused message.</summary>
    /// <param name="term">The durable term of the copy.</param>
    /// <param name="refusalCode">The stable refusal marker.</param>
    /// <returns>The reply.</returns>
    internal static ReplicaTransferReply Refused(ulong term, string refusalCode) => new(term, false, refusalCode, false);
}
