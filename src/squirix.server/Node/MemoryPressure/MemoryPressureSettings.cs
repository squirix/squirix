namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Partial settings shape for <c>Squirix.settings.json</c> <c>MemoryPressure</c> section (nullable fields merge onto defaults).
/// </summary>
internal sealed class MemoryPressureSettings
{
    public int? CriticalPressureThresholdPercent { get; init; }

    public bool? Enabled { get; init; }

    public int? HighPressureThresholdPercent { get; init; }

    public long? MaxEstimatedCacheBytes { get; init; }

    public bool? RejectWritesOnCriticalPressure { get; init; }

    /// <summary>
    /// Merges these settings onto a baseline (JSON <see langword="null" /> fields keep baseline values).
    /// </summary>
    /// <param name="baseline">Baseline options.</param>
    /// <returns>Merged options.</returns>
    public MemoryPressureOptions MergeInto(MemoryPressureOptions baseline) => new()
    {
        Enabled = Enabled ?? baseline.Enabled,
        MaxEstimatedCacheBytes = CoalesceMaxBytes(MaxEstimatedCacheBytes, baseline.MaxEstimatedCacheBytes),
        HighPressureThresholdPercent = HighPressureThresholdPercent ?? baseline.HighPressureThresholdPercent,
        CriticalPressureThresholdPercent = CriticalPressureThresholdPercent ?? baseline.CriticalPressureThresholdPercent,
        RejectWritesOnCriticalPressure = RejectWritesOnCriticalPressure ?? baseline.RejectWritesOnCriticalPressure,
    };

    private static long? CoalesceMaxBytes(long? fromSettings, long? baseline) => !fromSettings.HasValue ? baseline : fromSettings.Value <= 0 ? null : fromSettings.Value;
}
