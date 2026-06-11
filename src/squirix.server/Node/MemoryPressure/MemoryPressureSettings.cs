namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Partial settings shape for <c>Squirix.settings.json</c> <c>MemoryPressure</c> section (nullable fields merge onto defaults).
/// </summary>
internal sealed class MemoryPressureSettings
{
    public int? CriticalPressureThresholdPercent { get; init; }

    public int? HighPressureThresholdPercent { get; init; }

    public long? MaxEstimatedCacheBytes { get; init; }

    /// <summary>
    /// Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).
    /// </summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    public UnresolvedMemoryPressureOptions MergeInto(UnresolvedMemoryPressureOptions baseline) => new()
    {
        MaxEstimatedCacheBytes = MaxEstimatedCacheBytes ?? baseline.MaxEstimatedCacheBytes,
        HighPressureThresholdPercent = HighPressureThresholdPercent ?? baseline.HighPressureThresholdPercent,
        CriticalPressureThresholdPercent = CriticalPressureThresholdPercent ?? baseline.CriticalPressureThresholdPercent,
    };
}
