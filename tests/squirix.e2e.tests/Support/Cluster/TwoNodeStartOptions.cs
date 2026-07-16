using System;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.E2ETests.Support.Cluster;

/// <summary>Optional startup settings for two-node E2E clusters.</summary>
internal sealed class TwoNodeStartOptions
{
    /// <summary>Gets the inter-node mTLS profile for node B.</summary>
    public MtlsTestNodeProfile NodeBProfile { get; init; } = MtlsTestNodeProfile.Normal;

    /// <summary>Gets optional external auth settings applied to both nodes.</summary>
    public TestNodeSecurityOptions? Security { get; init; }

    /// <summary>Gets the inter-node mTLS profile for node A.</summary>
    internal MtlsTestNodeProfile NodeAProfile { get; init; } = MtlsTestNodeProfile.Normal;

    internal MtlsTestNodeProfile GetProfile(string nodeId) => nodeId switch
    {
        "nodeA" => NodeAProfile,
        "nodeB" => NodeBProfile,
        _ => throw new ArgumentOutOfRangeException(nameof(nodeId), nodeId, "Unsupported E2E node identifier."),
    };
}
