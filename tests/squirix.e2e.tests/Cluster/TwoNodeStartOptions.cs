using System;
using Squirix.Attributes;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.E2ETests.Cluster;

/// <summary>Optional startup settings for two-node E2E clusters.</summary>
[Immutable]
internal sealed class TwoNodeStartOptions
{
    /// <summary>Gets the inter-node mTLS profile for node A.</summary>
    internal TestNodeProfile NodeAProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the inter-node mTLS profile for node B.</summary>
    internal TestNodeProfile NodeBProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets optional external auth settings applied to both nodes.</summary>
    internal TestNodeSecurityOptions? Security { get; init; }

    /// <summary>Gets the shared node time source applied to every node; null keeps the real system clock.</summary>
    internal TimeProvider? TimeProvider { get; init; }

    internal TestNodeProfile GetProfile(string nodeId) => nodeId switch
    {
        "nodeA" => NodeAProfile,
        "nodeB" => NodeBProfile,
        _ => throw new ArgumentOutOfRangeException(nameof(nodeId), nodeId, "Unsupported E2E node identifier."),
    };
}
