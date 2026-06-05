using System;

namespace Squirix.E2ETests.Infrastructure.Stress;

/// <summary>
/// Immutable description of a stress workload: writer count, per-writer operation count, and a hard time budget.
/// </summary>
internal readonly struct StressLoadProfile
{
    public StressLoadProfile(int writers, TimeSpan budget)
    {
        Writers = writers;
        Budget = budget;
    }

    /// <summary>Gets the hard deadline after which the workload is considered hung.</summary>
    public TimeSpan Budget { get; }

    /// <summary>Gets the number of concurrent writers.</summary>
    public int Writers { get; }
}
