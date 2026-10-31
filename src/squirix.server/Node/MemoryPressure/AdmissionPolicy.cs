using System.Diagnostics;

namespace Squirix.Server.Node.MemoryPressure;

/// <summary>Decides admission of memory-growing operations from usage and pressure thresholds.</summary>
internal static class AdmissionPolicy
{
    /// <summary>Decides admission of a memory-growing write.</summary>
    /// <param name="evaluator">Evaluates pressure for a byte count.</param>
    /// <param name="accounting">Supplies the current byte count.</param>
    /// <param name="estimatedNetGrowthBytes">Best-effort net growth in estimated resident bytes; a negative value counts as no growth.</param>
    /// <param name="magnitudeUnknown">When <see langword="true" />, the net growth cannot be bounded cheaply.</param>
    /// <returns>The rejection reason, or <see cref="AdmissionRejection.None" /> when admitted.</returns>
    internal static AdmissionRejection Evaluate(IMemoryPressureStateEvaluator evaluator, IMemoryUsageAccounting accounting, long estimatedNetGrowthBytes, bool magnitudeUnknown)
    {
        var boundedGrowth = estimatedNetGrowthBytes < 0 ? 0 : estimatedNetGrowthBytes;
        if (!magnitudeUnknown && boundedGrowth == 0)
            return AdmissionRejection.None;

        var currentBytes = accounting.ReadEstimatedBytes();
        return (magnitudeUnknown, evaluator.Evaluate(currentBytes) == PressureLevel.Critical) switch
        {
            (true, true) => AdmissionRejection.UnknownSize,
            (true, false) => AdmissionRejection.None,
            (false, true) => AdmissionRejection.CriticalPressure,
            (false, false) => evaluator.Evaluate(AddSaturating(currentBytes, boundedGrowth)) == PressureLevel.Critical ? AdmissionRejection.EstimatedLimit : AdmissionRejection.None,
        };
    }

    private static long AddSaturating(long left, long right)
    {
        Debug.Assert(right > 0, "Saturating addition expects a positive growth.");
        return left > long.MaxValue - right ? long.MaxValue : left + right;
    }
}
