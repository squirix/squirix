using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>
/// Tests for <see cref="ClusterPeerChannelAddress" />.
/// </summary>
public sealed class ClusterPeerChannelAddressTests
{
    /// <summary>
    /// Ensures pooled cluster clients use the primary peer URL when inter-node mTLS is disabled.
    /// </summary>
    [Fact]
    public void ResolveUsesPrimaryUrlWhenInterNodeMtlsDisabled()
    {
        var peer = new Peer { NodeId = "node-a", Url = "https://localhost:6001" };

        var address = ClusterPeerChannelAddress.Resolve(peer, new MtlsOptions { InternalListenPort = 6101 }, false);

        Assert.Equal(peer.Url, address);
    }

    /// <summary>
    /// Ensures pooled cluster clients prefer the configured inter-node URL when mTLS is enabled.
    /// </summary>
    [Fact]
    public void ResolveUsesInterNodeUrlWhenConfigured()
    {
        var peer = new Peer
        {
            NodeId = "node-b",
            Url = "https://localhost:6001",
            InterNodeUrl = "https://localhost:6202",
        };

        var address = ClusterPeerChannelAddress.Resolve(peer, new MtlsOptions { InternalListenPort = 6101 }, true);

        Assert.Equal("https://localhost:6202", address);
    }

    /// <summary>
    /// Ensures pooled cluster clients fall back to the local internal listen port when no inter-node URL is configured.
    /// </summary>
    [Fact]
    public void ResolveUsesConfiguredInternalListenPortWhenInterNodeUrlMissing()
    {
        var peer = new Peer { NodeId = "node-b", Url = "https://127.0.0.1:6001" };

        var address = ClusterPeerChannelAddress.Resolve(peer, new MtlsOptions { InternalListenPort = 6101 }, true);

        Assert.Equal("https://127.0.0.1:6101/", address);
    }
}
