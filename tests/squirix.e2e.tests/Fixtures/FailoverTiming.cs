using Squirix.Server.TestKit.Hosting;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Election timing of the failover end-to-end tests: the pull request tier, with a jitter seed fixed per test.</summary>
internal static class FailoverTiming
{
    /// <summary>Gets the pull request tier timing whose jitter seed is a stable hash of the test name; the testkit mixes in the node.</summary>
    /// <param name="testName">The test name.</param>
    /// <returns>The election timing of every node of the test cluster.</returns>
    internal static TestElectionTiming For(string testName)
    {
        // FNV-1a: string.GetHashCode is randomized per process, and the seed must repeat across runs.
        var hash = 14695981039346656037UL;
        foreach (var c in testName)
            hash = unchecked((hash ^ c) * 1099511628211UL);

        return new TestElectionTiming { JitterSeed = hash };
    }
}
