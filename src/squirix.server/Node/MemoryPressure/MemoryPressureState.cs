namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Represents coarse memory pressure derived from configured limits and estimated usage.
/// </summary>
internal enum MemoryPressureState
{
    /// <summary>
    /// Below the configured high-pressure threshold (including zero estimated usage).
    /// </summary>
    Normal,

    /// <summary>
    /// At or above the high threshold and below the critical threshold.
    /// </summary>
    High,

    /// <summary>
    /// At or above the critical threshold.
    /// </summary>
    Critical,
}
