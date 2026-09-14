using System;
using Squirix.Attributes;

namespace Squirix.E2ETests;

/// <summary>Immutable description of a stress workload: writer count, per-writer operation count, and a hard time budget.</summary>
[Immutable]
internal sealed record LoadProfile
{
    internal LoadProfile(int writers, TimeSpan budget)
    {
        Writers = writers;
        Budget = budget;
    }

    /// <summary>Gets the hard deadline after which the workload is considered hung.</summary>
    internal TimeSpan Budget { get; }

    /// <summary>Gets the number of concurrent writers.</summary>
    internal int Writers { get; }
}
