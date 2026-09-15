using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster;

/// <summary>Activation-gate coverage for RF&gt;1 prerequisites and RF=1 planning registration.</summary>
[Immutable]
public sealed class ReplicationActivationTests : ServerUnitTestBase
{
    /// <summary>RF=1 registers planning services with network replication disabled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RfOneDoesNotRegisterReplicationServices(CancellationToken cancellationToken)
    {
        var uri = ListenPortPool.ServerUnitTests.NextHttpUri();
        await using var host = await TestNodeHostFactory.StartNodeAsync("n1", uri, cancellationToken);
        var featureState = host.Services.GetRequiredService<FeatureState>();
        _ = await Assert.That(featureState.NetworkReplicationEnabled).IsFalse();
        _ = host.Services.GetRequiredService<IReplicaGroupLocator>();
        _ = host.Services.GetRequiredService<PhysicalNodeRing>();
    }

    /// <summary>RF=2 with both prerequisites present activates networking with no failures.</summary>
    [Test]
    public async Task RfTwoActivatesWithPrerequisites()
    {
        var failures = new List<string>();
        ReplicationActivationGuard.CollectFailures(failures, 2, true, true, true);
        _ = await Assert.That(failures).IsEmpty();
    }

    /// <summary>RF=2 without both prerequisites reports ordered configuration failures.</summary>
    [Test]
    public async Task RfTwoRequiresPersistenceAndMtls()
    {
        var missingPersistence = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingPersistence, 2, false, false, true);
        await SequenceAssert.Equal([ReplicationActivationGuard.PersistenceRequired], missingPersistence, StringComparer.Ordinal);

        var missingMtls = new List<string>();
        ReplicationActivationGuard.CollectFailures(missingMtls, 2, true, false, true);
        await SequenceAssert.Equal([ReplicationActivationGuard.MtlsRequired], missingMtls, StringComparer.Ordinal);
    }
}
