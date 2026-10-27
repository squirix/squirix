namespace Squirix.Server.TestKit.Hosting;

/// <summary>A phase of a failover a <see cref="FailoverTimeline{TOptions}" /> records the first sample of.</summary>
internal enum FailoverPhase
{
    /// <summary>No running node holds authority in the term the timeline started in.</summary>
    LeaderLost = 0,

    /// <summary>A running node saw a term above the one the timeline started in.</summary>
    TermRaised = 1,

    /// <summary>A running node holds authority in a term above the one the timeline started in.</summary>
    NewLeader = 2,

    /// <summary>Every running node serving the group acts in the term of the new leader and routes to it.</summary>
    Converged = 3,
}
