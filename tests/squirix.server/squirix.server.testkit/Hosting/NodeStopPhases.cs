namespace Squirix.Server.TestKit.Hosting;

/// <summary>Where the time of one node stop went, as the test host observed it; times are in milliseconds from the start of the stop.</summary>
/// <param name="HostStopMs">The time the web server and the hosted services needed to stop; for an abrupt shutdown, the time to drop the listeners.</param>
/// <param name="DisposeMs">The time to dispose the application and its container.</param>
/// <param name="PersistenceReleaseMs">The time spent waiting until the journal and snapshot files were released; zero without persistence.</param>
/// <param name="TotalMs">The time of the whole stop, including the parts above and the disposal of the owned scope.</param>
/// <param name="InFlightAtStop">The number of requests being served when the stop began.</param>
/// <param name="LastRequestFinishedMs">The time from the start of the stop until the last request finished, or <see langword="null" /> when none finished afterwards.</param>
public sealed record NodeStopPhases(
    double HostStopMs,
    double DisposeMs,
    double PersistenceReleaseMs,
    double TotalMs,
    int InFlightAtStop,
    double? LastRequestFinishedMs);
