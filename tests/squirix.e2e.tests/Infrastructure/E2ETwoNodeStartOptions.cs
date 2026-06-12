using System;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Cluster;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Optional startup settings for two-node E2E clusters.
/// </summary>
public sealed class E2ETwoNodeStartOptions
{
    /// <summary>
    /// Gets optional external auth settings applied to both nodes.
    /// </summary>
    public TestNodeSecurityOptions? Security { get; init; }

    /// <summary>
    /// Gets the inter-node mTLS profile for node A.
    /// </summary>
    public MtlsTestNodeProfile NodeAProfile { get; init; } = MtlsTestNodeProfile.Normal;

    /// <summary>
    /// Gets the inter-node mTLS profile for node B.
    /// </summary>
    public MtlsTestNodeProfile NodeBProfile { get; init; } = MtlsTestNodeProfile.Normal;

    internal MtlsTestNodeProfile GetProfile(string nodeId) =>
        nodeId switch
        {
            "nodeA" => NodeAProfile,
            "nodeB" => NodeBProfile,
            _ => throw new ArgumentOutOfRangeException(nameof(nodeId), nodeId, "Unsupported E2E node identifier."),
        };
}
