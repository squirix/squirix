using System;
using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Election timing of the failover end-to-end tests: the pull request tier, or the product defaults, with a jitter seed fixed per test.</summary>
internal static class FailoverTiming
{
    /// <summary>The longest wait of a node without a leader in <see cref="ForRefusal" />, shorter than the derived default to keep the minority tests fast.</summary>
    private static readonly TimeSpan RefusalLeaderWait = TimeSpan.FromSeconds(1);

    /// <summary>Gets the pull request tier timing whose jitter seed is a stable hash of the test name; the testkit mixes in the node.</summary>
    /// <param name="testName">The test name.</param>
    /// <returns>The election timing of every node of the test cluster.</returns>
    internal static TestElectionTiming For(string testName) => new() { JitterSeed = SeedOf(testName) };

    /// <summary>Gets the pull request tier timing for a test that waits on a node refusing for lack of a leader, with the wait cut short for speed.</summary>
    /// <remarks>The derived cap of the default leader wait is covered by <c language="csharp">NoQuorumFailoverTests</c> and <c language="csharp">ReplicaLeaderTableTests</c>.</remarks>
    /// <param name="testName">The test name, which seeds the election jitter.</param>
    /// <returns>The election timing of every node of the test cluster.</returns>
    internal static TestElectionTiming ForRefusal(string testName) => For(testName) with { LeaderWaitTimeout = RefusalLeaderWait };

    /// <summary>Gets the timing a node runs without an override, as the testkit copies it from the product, with a jitter seed fixed per test.</summary>
    /// <param name="testName">The test name, which seeds the election jitter; the testkit mixes in the node.</param>
    /// <returns>The election timing of every node of the test cluster.</returns>
    internal static TestElectionTiming ProductDefaults(string testName) => TestElectionTiming.ProductDefaults with { JitterSeed = SeedOf(testName) };

    /// <summary>Gets a stable hash of a name, so a seed repeats across runs.</summary>
    /// <param name="name">The name.</param>
    /// <returns>The hash.</returns>
    internal static ulong SeedOf(string name)
    {
        // FNV-1a: string.GetHashCode is randomized per process, and the seed must repeat across runs.
        var hash = 14695981039346656037UL;
        foreach (var c in name)
            hash = unchecked((hash ^ c) * 1099511628211UL);

        return hash;
    }
}
