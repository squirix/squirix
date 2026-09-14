using System;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Validation tests for <see cref="ReplicaRpcGateway" /> construction.</summary>
[Immutable]
public sealed class ReplicaRpcGatewayTests : ServerUnitTestBase
{
    /// <summary>Verifies that the gateway requires a client pool.</summary>
    [Fact]
    public void GatewayRequiresPool()
    {
        IServerClientPool? pool = null;

        _ = NodeExceptionAssert.For<ArgumentNullException>().Throws(pool, static p => _ = new ReplicaRpcGateway(p!));
    }
}
