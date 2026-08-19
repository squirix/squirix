using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Activation-gate coverage for RF&gt;1 prerequisites and RF=1 planning registration.</summary>
[Immutable]
public sealed class ReplicationActivationTests : ServerUnitTestBase
{
    /// <summary>RF=2 without both prerequisites reports ordered configuration failures.</summary>
    [Fact]
    public void RfTwoRequiresPersistenceAndMtls()
    {
        var missingPersistence = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingPersistence, 2, false, false);
        Assert.Equal([ReplicationActivationGuard.PersistenceRequired], missingPersistence);

        var missingMtls = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingMtls, 2, true, false);
        Assert.Equal([ReplicationActivationGuard.MtlsRequired], missingMtls);
    }

    /// <summary>RF=2 with prerequisites still refuses activation until M8-09.</summary>
    [Fact]
    public void RfTwoRemainsDisabledBeforeActivation()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, true);
        Assert.Equal([ReplicationActivationGuard.NotActivated], failures);
    }

    /// <summary>RF=1 registers planning services with network replication disabled.</summary>
    [Fact]
    public async Task RfOneDoesNotRegisterReplicationServices()
    {
        var uri = ListenPortPool.ServerUnitTests.NextHttpUri();
        await using var host = await TestNodeHostFactory.StartNodeAsync("n1", uri, DefaultCancellationToken);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        Assert.False(featureState.NetworkReplicationEnabled);
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
        _ = host.Services.GetRequiredService<PhysicalNodeRing>();
    }
}
