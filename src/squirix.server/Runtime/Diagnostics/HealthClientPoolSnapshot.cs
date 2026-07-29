namespace Squirix.Server.Runtime.Diagnostics;

/// <summary>Client pool subsection of health-ready diagnostics.</summary>
/// <param name="Enabled">Whether the outbound client pool is active.</param>
/// <param name="PeerCount">Number of configured cluster peers.</param>
internal readonly record struct HealthClientPoolSnapshot(bool Enabled, int PeerCount);
