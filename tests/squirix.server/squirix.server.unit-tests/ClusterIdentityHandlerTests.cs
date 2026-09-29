using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>The shared test identity owns the per-peer handlers its outbound handler factories create.</summary>
public sealed class ClusterIdentityHandlerTests
{
    /// <summary>A peer handler stays usable while the identity lives and is disposed together with the identity.</summary>
    /// <param name="profile">A profile that wires a custom outbound handler factory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(TestNodeProfile.NoOutboundClientCertificate)]
    [Arguments(TestNodeProfile.UntrustedOutboundClientCertificate)]
    [Arguments(TestNodeProfile.ExpiredPeerCertificate)]
    public async Task DisposeReleasesPeerHandlers(TestNodeProfile profile, CancellationToken cancellationToken)
    {
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        var identity = new ClusterIdentity();
        try
        {
            var shared = identity;
            var peers = ClusterIdentity.CreatePeers(
                [new ClusterNode("nodeA", primaryA.HttpUri), new ClusterNode("nodeB", primaryB.HttpUri)],
                ref shared);
            var cluster = new TopologyOptions(peers) { NodeId = "nodeA", Uri = primaryA.HttpUri };
            var startup = await identity.ResolveNodeStartupForBindAsync(cluster, profile, cancellationToken);
            using var material = startup.Certificate;
            var created = await Assert.That(startup.PeerHandlerFactory?.Invoke("nodeB")).IsTypeOf<SocketsHttpHandler>();
            var handler = ThrowHelper.Required(created, "Expected the custom outbound handler factory to create a handler.");

            // A handler that has not sent a request still accepts configuration until it is disposed.
            handler.MaxConnectionsPerServer = 2;

            identity.Dispose();

            var ex = NodeExceptionAssert.For<ObjectDisposedException>().Throws(handler, static h => h.MaxConnectionsPerServer = 3);
            _ = await Assert.That(ex.ObjectName).Contains(nameof(SocketsHttpHandler), StringComparison.Ordinal);
        }
        finally
        {
            identity.Dispose();
        }
    }
}
