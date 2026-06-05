namespace Squirix.Server.Runtime.Contracts;

/// <summary>
/// Memory-pressure subsection of health-ready diagnostics.
/// </summary>
internal readonly record struct HealthMemoryPressureSnapshot(
    string State,
    long? MaxEstimatedCacheBytes,
    long EstimatedBytes,
    long EntryCount,
    long RejectedWriteCount,
    bool WriteRejectionActive);
