namespace Squirix.Server.Runtime.Contracts;

/// <summary>Memory-pressure subsection of health-ready diagnostics.</summary>
/// <param name="State">Current memory-pressure state label.</param>
/// <param name="MaxEstimatedCacheBytes">Configured cache byte budget used for pressure thresholds.</param>
/// <param name="EstimatedBytes">Estimated cache bytes in use.</param>
/// <param name="EntryCount">Estimated number of cache entries.</param>
/// <param name="RejectedWriteCount">Number of writes rejected due to memory pressure.</param>
/// <param name="WriteRejectionActive">Whether write rejection is currently active.</param>
internal readonly record struct HealthMemoryPressureSnapshot(
    string State,
    long MaxEstimatedCacheBytes,
    long EstimatedBytes,
    long EntryCount,
    long RejectedWriteCount,
    bool WriteRejectionActive);
