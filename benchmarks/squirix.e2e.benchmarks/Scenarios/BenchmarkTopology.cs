namespace Squirix.E2EBenchmarks.Scenarios;

/// <summary>End-to-end topology shape measured by a benchmark scenario.</summary>
public enum BenchmarkTopology
{
    /// <summary>One Squirix node and one client connected to that node.</summary>
    SingleNode = 0,

    /// <summary>Two nodes with a node A client and keys owned by node A.</summary>
    TwoNodeLocalOwner = 1,

    /// <summary>Two nodes with a node A client and keys owned by node B.</summary>
    TwoNodeRemoteOwner = 2,

    /// <summary>Two nodes with keys distributed across both owners.</summary>
    TwoNodeUniformKeys = 3,

    /// <summary>Two nodes with a small hot keyset distributed across both owners.</summary>
    TwoNodeHotKeys = 4,
}
