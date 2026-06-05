namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Client pool subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthClientPoolSnapshot(bool Enabled, int PeerCount);
