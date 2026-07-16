namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Represents coarse memory pressure derived from configured limits and estimated usage.</summary>
internal enum PressureLevel
{
    /// <summary>Below the configured high-pressure threshold (including zero estimated usage).</summary>
    Normal = 0,

    /// <summary>At or above the high threshold and below the critical threshold.</summary>
    High = 1,

    /// <summary>At or above the critical threshold.</summary>
    Critical = 2,
}
