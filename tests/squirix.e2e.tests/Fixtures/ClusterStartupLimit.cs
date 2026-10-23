using System;
using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Caps concurrent cluster-backed tests so node startups never stampede.</summary>
/// <remarks>Cold Debug host builds and certificate setup are CPU-heavy; two concurrent startups fit even small CI agents.</remarks>
[Immutable]
public sealed class ClusterStartupLimit : IParallelLimit
{
    /// <summary>Gets the maximum number of concurrent cluster-backed tests in the limited test bases.</summary>
    private const int MaxConcurrentStartups = 2;

    private const int MaxHostStartups = 4;
    private const int ProcessorsPerHostStartup = 6;

    /// <inheritdoc />
    public int Limit => MaxConcurrentStartups;

    /// <summary>
    /// Gets the maximum number of cluster startups in progress at once across the process: two on small machines, up to four on machines
    /// with many cores, where startups queued behind two slots otherwise delay every cluster-backed test.
    /// </summary>
    internal static int MaxConcurrentHostStartups { get; } =
        Math.Clamp(Environment.ProcessorCount / ProcessorsPerHostStartup, MaxConcurrentStartups, MaxHostStartups);
}
