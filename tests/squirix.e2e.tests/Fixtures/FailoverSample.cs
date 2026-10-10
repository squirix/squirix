namespace Squirix.E2ETests.Fixtures;

/// <summary>What one leader stop of a series recorded; times are in milliseconds, and each says what it is measured from.</summary>
/// <param name="Iteration">The position in the series, from one.</param>
/// <param name="StoppedLeader">The leader that stopped.</param>
/// <param name="NewLeader">The node that acknowledged the recovery probe.</param>
/// <param name="NewTerm">The term of that node.</param>
/// <param name="RecoveryFromStopStartMs">The time from the start of the stop until a write and a read through a surviving node succeeded; it includes the time the stop takes.</param>
/// <param name="StopDurationMs">The time from the start of the stop until the stopped node was down.</param>
/// <param name="RecoverySinceDownMs">The time from the moment the stopped node was down until that write and read succeeded; the part of the recovery the cluster controls.</param>
/// <param name="LeaderLostMs">The time from the start of the stop until no node held authority in the old term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="TermRaisedMs">The time from the start of the stop until a node saw a higher term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="NewLeaderMs">The time from the start of the stop until a node held authority in a higher term, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="ConvergedMs">The time from the start of the stop until every survivor followed the new leader, or <see langword="null" /> when the timeline did not see it.</param>
/// <param name="StopHostMs">The part of the stop spent stopping the web server and the hosted services, or <see langword="null" /> when the host did not report it.</param>
/// <param name="StopDisposeMs">The part of the stop spent disposing the application, or <see langword="null" /> when the host did not report it.</param>
/// <param name="StopPersistenceReleaseMs">The part of the stop spent waiting until the journal files were released, or <see langword="null" /> when the host did not report it.</param>
/// <param name="StopTotalMs">The time the host measured for the whole stop, or <see langword="null" /> when the host did not report it.</param>
/// <param name="StopInFlightRequests">The number of requests the stopping node was serving when the stop began, or <see langword="null" /> when the host did not report it.</param>
/// <param name="StopLastRequestFinishedMs">The time from the start of the stop until the last request on the stopping node finished, or <see langword="null" /> when none finished afterwards.</param>
internal sealed record FailoverSample(
    int Iteration,
    string StoppedLeader,
    string NewLeader,
    ulong NewTerm,
    double RecoveryFromStopStartMs,
    double StopDurationMs,
    double RecoverySinceDownMs,
    double? LeaderLostMs,
    double? TermRaisedMs,
    double? NewLeaderMs,
    double? ConvergedMs,
    double? StopHostMs = null,
    double? StopDisposeMs = null,
    double? StopPersistenceReleaseMs = null,
    double? StopTotalMs = null,
    int? StopInFlightRequests = null,
    double? StopLastRequestFinishedMs = null);
