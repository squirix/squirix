namespace Squirix.Server.Storage.Snapshot;

/// <summary>Decides when critical memory pressure may defer background snapshot work.</summary>
internal static class SnapshotDurabilityPolicy
{
    /// <summary>
    /// Returns whether a background snapshot attempt should be deferred solely because critical memory pressure is active.
    /// Durability-critical volume/ops triggers and the first
    /// in-process snapshot are never deferred.
    /// </summary>
    /// <param name="isCriticalMemoryPressure">Whether the current memory pressure state is critical.</param>
    /// <param name="hasPublishedSnapshotInProcess">Whether this coordinator has published a snapshot in the current process.</param>
    /// <param name="opsDelta">Journal ops appended since the last snapshot watermark.</param>
    /// <param name="bytesDelta">Journal bytes appended since the last snapshot watermark.</param>
    /// <param name="options">Snapshot trigger thresholds.</param>
    /// <returns><see langword="true" /> when the snapshot attempt should be deferred.</returns>
    internal static bool ShouldDeferSnapshotUnderCriticalMemoryPressure(
        bool isCriticalMemoryPressure,
        bool hasPublishedSnapshotInProcess,
        long opsDelta,
        long bytesDelta,
        TriggerOptions options)
    {
        if (!isCriticalMemoryPressure)
            return false;

        if (!hasPublishedSnapshotInProcess)
            return false;

        if (options.SnapshotEveryNBytes > 0 && bytesDelta >= options.SnapshotEveryNBytes)
            return false;

        if (options.SnapshotEveryNOps > 0 && opsDelta >= options.SnapshotEveryNOps)
            return false;

        return true;
    }
}
