namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Composition-layer admission gate for memory-growing cache writes under critical pressure (v0.7.3).
/// </summary>
internal interface IMemoryPressureGate
{
    /// <summary>
    /// Throws an internal memory-pressure admission exception when the operation must not proceed because
    /// memory pressure policy rejects a growing mutation at critical pressure.
    /// </summary>
    /// <param name="estimatedNetGrowthBytes">Best-effort non-negative net growth in estimated resident bytes.</param>
    /// <param name="magnitudeUnknown">
    /// When <see langword="true" />, the net growth cannot be bounded cheaply; policy treats this conservatively at critical pressure.
    /// </param>
    /// <param name="operation">Bounded admission operation label for metrics (never a cache name or key).</param>
    void ThrowIfMemoryGrowingWriteRejected(long estimatedNetGrowthBytes, bool magnitudeUnknown, string operation);
}
