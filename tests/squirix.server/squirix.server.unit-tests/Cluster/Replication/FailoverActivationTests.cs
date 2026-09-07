using System;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Automatic failover stays disabled until failover activation.</summary>
[Immutable]
public sealed class FailoverActivationTests : ServerUnitTestBase
{
    /// <summary>The internal failover switch defaults off for any replica factor and no feature state enables it.</summary>
    [Fact]
    public void AutomaticFailoverRemainsDisabled()
    {
        var uri = new Uri("https://localhost:6001");
        var single = new TopologyOptions(new ServerPeer { NodeId = "node-a", Uri = uri })
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
        };
        Assert.False(single.AutomaticFailoverEnabled);

        var triple = new TopologyOptions(
            [
                new ServerPeer { NodeId = "node-a", Uri = uri },
                new ServerPeer { NodeId = "node-b", Uri = uri },
                new ServerPeer { NodeId = "node-c", Uri = uri },
            ])
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
            ReplicaCount = 3,
        };
        Assert.False(triple.AutomaticFailoverEnabled);

        Assert.Equal(new FeatureState(false, false), FeatureState.Disabled);
        Assert.Equal(new FeatureState(false, true), FeatureState.Foundation);
        Assert.Equal(new FeatureState(true, false), FeatureState.Activated);
    }
}
