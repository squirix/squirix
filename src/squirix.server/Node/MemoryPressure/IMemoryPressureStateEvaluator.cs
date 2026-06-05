namespace Squirix.Server.Node.MemoryPressure;

/// <summary>
/// Computes <see cref="MemoryPressureState" /> from configured limits and estimated usage (independent from transport backpressure).
/// </summary>
internal interface IMemoryPressureStateEvaluator
{
    /// <summary>
    /// Evaluates pressure state for the given estimated cache usage in bytes.
    /// </summary>
    /// <param name="estimatedCacheBytes">Estimated bytes currently attributed to the cache; must be non-negative.</param>
    /// <returns>The derived <see cref="MemoryPressureState" />.</returns>
    MemoryPressureState Evaluate(long estimatedCacheBytes);
}
