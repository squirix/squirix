namespace Squirix.E2ETests.Fixtures;

/// <summary>What one leader stop of a series recorded; times are in milliseconds from the start of the stop.</summary>
/// <param name="Iteration">The position in the series, from one.</param>
/// <param name="StoppedLeader">The leader that stopped.</param>
/// <param name="NewLeader">The node that acknowledged the recovery probe.</param>
/// <param name="NewTerm">The term of that node.</param>
/// <param name="RecoveryMs">The time until a write and a read through a surviving node succeeded.</param>
/// <param name="StopMs">The time until the stopped node was down.</param>
/// <param name="LeaderLostMs">The time until no node held authority in the old term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="TermRaisedMs">The time until a node saw a higher term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="NewLeaderMs">The time until a node held authority in a higher term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="ConvergedMs">The time until every survivor followed the new leader, or <see langword="null" /> when the timeline did not see it.</param>
internal sealed record FailoverSample(
    int Iteration,
    string StoppedLeader,
    string NewLeader,
    ulong NewTerm,
    double RecoveryMs,
    double StopMs,
    double? LeaderLostMs,
    double? TermRaisedMs,
    double? NewLeaderMs,
    double? ConvergedMs);
