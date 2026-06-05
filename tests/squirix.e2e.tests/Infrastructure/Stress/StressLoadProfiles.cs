using System;
using System.Globalization;

namespace Squirix.E2ETests.Infrastructure.Stress;

/// <summary>
/// Named stress workloads. Operation counts scale with <c>SQUIRIX_STRESS_SCALE</c> so the repeat runner can dial
/// intensity without recompiling; DEBUG builds default to a low scale to keep local runs fast.
/// </summary>
internal static class StressLoadProfiles
{
    private const string ScaleVariable = "SQUIRIX_STRESS_SCALE";

    /// <summary>Gets the mixed-mutation contention workload over a fixed key set.</summary>
    public static StressLoadProfile MixedMutation { get; } = new(6, TimeSpan.FromSeconds(120));

    /// <summary>Gets the effective operation-count multiplier.</summary>
    private static double Scale { get; } = ResolveScale();

    /// <summary>
    /// Scales a base operation count by <see cref="Scale" />, never returning less than one.
    /// </summary>
    /// <param name="baseOperations">The unscaled operation count.</param>
    /// <returns>The scaled operation count.</returns>
    public static int ScaleOperations(int baseOperations)
    {
        var scaled = (int)Math.Round(baseOperations * Scale, MidpointRounding.AwayFromZero);
        return Math.Max(1, scaled);
    }

    private static double ResolveScale()
    {
        var raw = Environment.GetEnvironmentVariable(ScaleVariable);
        if (!string.IsNullOrWhiteSpace(raw) && double.TryParse(raw, NumberStyles.Float, CultureInfo.InvariantCulture, out var parsed) && parsed > 0d)
        {
            return parsed;
        }

#if DEBUG
        return 0.1d;
#else
        return 1d;
#endif
    }
}
