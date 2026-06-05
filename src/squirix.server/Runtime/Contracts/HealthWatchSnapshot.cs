namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Watch subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthWatchSnapshot(bool Enabled, int ActiveWatches, int BufferedEvents, int DroppedEvents);
