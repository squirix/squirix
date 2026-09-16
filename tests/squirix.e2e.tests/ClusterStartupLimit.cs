using Squirix.Attributes;
using TUnit.Core.Interfaces;

namespace Squirix.E2ETests;

/// <summary>Caps concurrent cluster-backed tests so node startups never stampede.</summary>
/// <remarks>Cold Debug host builds and RSA key generation are CPU-heavy; two concurrent startups fit even small CI agents.</remarks>
[Immutable]
public sealed class ClusterStartupLimit : IParallelLimit
{
    /// <summary>Gets the maximum number of concurrent cluster startups.</summary>
    internal const int MaxConcurrentStartups = 2;

    /// <inheritdoc />
    public int Limit => MaxConcurrentStartups;
}
