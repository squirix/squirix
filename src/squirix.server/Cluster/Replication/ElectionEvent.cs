namespace Squirix.Server.Cluster.Replication;

/// <summary>What one step of the election driver of a group did, for the service that runs it to report.</summary>
internal enum ElectionEvent
{
    /// <summary>Nothing changed.</summary>
    None = 0,

    /// <summary>The election timeout fired, but the node may not campaign now; see the denial.</summary>
    Denied = 1,

    /// <summary>The pre-vote found no majority; the term is unchanged.</summary>
    PreVoteLost = 2,

    /// <summary>The vote round in the term found no majority.</summary>
    VoteLost = 3,

    /// <summary>The node won the term, or took over the provisional term of its own group, and leads it without authority yet.</summary>
    Elected = 4,

    /// <summary>The leader-term entry of the term is committed: the leader has authority.</summary>
    Authorized = 5,

    /// <summary>The leader-term entry is not committed yet; the promotion is retried.</summary>
    PromotionPending = 6,

    /// <summary>The leader stepped down: a higher term, no majority contact within the election timeout, or no authority within its deadline.</summary>
    SteppedDown = 7,

    /// <summary>The leader stepped down but could not retire yet; the retirement is retried.</summary>
    RetirePending = 8,

    /// <summary>A higher term was made durable and the node follows it.</summary>
    TermObserved = 9,

    /// <summary>A term the driver had to make durable could not be persisted; the step is retried.</summary>
    TermNotDurable = 10,

    /// <summary>A leader with authority stepped down because it applied no committed entry within its apply deadline; see the wait.</summary>
    ApplyTimedOut = 11,

    /// <summary>A leader with authority stepped down because its appends to its own group log kept failing for its append deadline; see the wait.</summary>
    AppendTimedOut = 12,
}
