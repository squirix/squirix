using System;

namespace Squirix.Server.TestKit.Utils;

/// <summary>
/// Helpers for measuring allocations in tests.
/// </summary>
public static class AllocationTestHelper
{
    /// <summary>
    /// Measures allocated bytes for an action using warmup and repeated iterations.
    /// </summary>
    /// <param name="action">The action to measure.</param>
    /// <param name="warmupIterations">Warmup iteration count.</param>
    /// <param name="measuredIterations">Measured iteration count.</param>
    /// <returns>Best (lowest) measured allocation in bytes.</returns>
    public static long MeasureAllocatedBytes(Action action, int warmupIterations = 3, int measuredIterations = 5)
    {
        ArgumentNullException.ThrowIfNull(action);

        ArgumentOutOfRangeException.ThrowIfNegative(warmupIterations);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(measuredIterations);

        for (var i = 0; i < warmupIterations; i++)
            action();

        var best = long.MaxValue;
        for (var i = 0; i < measuredIterations; i++)
        {
            var allocated = MeasureAllocatedBytesOnce(action);
            if (allocated < best)
                best = allocated;
        }

        return best == long.MaxValue ? 0 : best;
    }

    private static long MeasureAllocatedBytesOnce(Action action)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        action();
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }
}
