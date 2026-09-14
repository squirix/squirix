using System;
using Squirix.Attributes;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.E2ETests.Cluster;

/// <summary>Optional startup settings for multi-node E2E clusters.</summary>
[Immutable]
internal sealed class MultiNodeStartOptions
{
    /// <summary>Gets the internode mTLS profile for node A.</summary>
    internal TestNodeProfile NodeAProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the internode mTLS profile for node B.</summary>
    internal TestNodeProfile NodeBProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the internode mTLS profile for node C.</summary>
    internal TestNodeProfile NodeCProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the replica factor applied to every node; 1 preserves single-copy routing.</summary>
    internal int ReplicaCount { get; init; } = 1;

    /// <summary>Gets optional external auth settings applied to both nodes.</summary>
    internal TestNodeSecurityOptions? Security { get; init; }

    /// <summary>Gets the shared node time source applied to every node; null keeps the real system clock.</summary>
    internal TimeProvider? TimeProvider { get; init; }

    internal TestNodeProfile GetProfile(string nodeId) => nodeId switch
    {
        "nodeA" => NodeAProfile,
        "nodeB" => NodeBProfile,
        "nodeC" => NodeCProfile,
        _ => throw new ArgumentOutOfRangeException(nameof(nodeId), nodeId, "Unsupported E2E node identifier."),
    };
}
