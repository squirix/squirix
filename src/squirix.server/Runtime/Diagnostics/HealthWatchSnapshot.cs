using Squirix.Attributes;

namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Watch subsection of health-ready diagnostics.</summary>
/// <param name="Enabled">Whether watch coordination is enabled.</param>
/// <param name="ActiveWatches">Number of active watches.</param>
/// <param name="BufferedEvents">Number of buffered watch events.</param>
/// <param name="DroppedEvents">Number of dropped watch events.</param>
[Immutable]
internal readonly record struct HealthWatchSnapshot(bool Enabled, int ActiveWatches, int BufferedEvents, int DroppedEvents);
