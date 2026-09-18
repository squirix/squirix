using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Protects the hold-open internal mTLS port discipline that keeps parallel multi-node tests from colliding (see #612).</summary>
public sealed class ClusterIdentityInternalPortTests
{
    /// <summary>An allocated internal port must stay bound until released, so a parallel test cannot grab it between probing and Kestrel bind.</summary>
    [Test]
    public async Task InternalPortStaysBoundUntilReleased()
    {
        using var identity = new ClusterIdentity();
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        var peers = CreateTwoNodePeers(identity, [primaryA.HttpUri, primaryB.HttpUri]);
        var internalPort = GetInterNodePort(peers[0]);

        _ = NodeExceptionAssert.For<SocketException>().Throws(internalPort, static port => BindExclusively(port));

        identity.Dispose();
        BindExclusively(internalPort);

        _ = await Assert.That(GetInterNodePort(peers[1]) != internalPort).IsTrue().Because("Sibling nodes must not share one internal listener port.");
    }

    /// <summary>Parallel cluster owners must receive distinct internal ports.</summary>
    [Test]
    public async Task ClustersGetDistinctInternalPorts()
    {
        const int ownerCount = 8;
        var seen = new HashSet<int>();
        var identities = new ClusterIdentity[ownerCount];
        try
        {
            for (var i = 0; i < ownerCount; i++)
            {
                identities[i] = new ClusterIdentity();
                using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
                using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
                var peers = CreateTwoNodePeers(identities[i], [primaryA.HttpUri, primaryB.HttpUri]);
                for (var p = 0; p < peers.Length; p++)
                {
                    var internalPort = GetInterNodePort(peers[p]);
                    _ = await Assert.That(seen.Add(internalPort)).IsTrue().Because($"Internal port {internalPort} was issued twice.");
                }
            }
        }
        finally
        {
            for (var i = 0; i < ownerCount; i++)
                identities[i].Dispose();
        }
    }

    /// <summary>A released internal port is reacquired live on restart so the reservation stays valid through certificate generation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartReacquiresReleasedInternalPort(CancellationToken cancellationToken)
    {
        using var identity = new ClusterIdentity();
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        var primaries = new[] { primaryA.HttpUri, primaryB.HttpUri };
        var peers = CreateTwoNodePeers(identity, primaries);
        var cluster = new TopologyOptions(peers) { NodeId = "nodeA", Uri = primaries[0] };

        var first = await identity.ResolveNodeStartupForBindAsync(cluster, TestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
        var firstPort = first.Options!.InternalListenPort;

        // Simulate a failed start / restart with the same identity: rebuilding the peers must
        // reacquire a live hold for the same assigned port before the retry generates certificates.
        var peersRetry = CreateTwoNodePeers(identity, primaries);
        var retryPort = GetInterNodePort(peersRetry[0]);
        _ = await Assert.That(retryPort).IsEqualTo(firstPort).Because("Restart must preserve the assigned internal port.");
        _ = NodeExceptionAssert.For<SocketException>().Throws(retryPort, static port => BindExclusively(port));

        var retryCluster = new TopologyOptions(peersRetry) { NodeId = "nodeA", Uri = primaries[0] };
        var second = await identity.ResolveNodeStartupForBindAsync(retryCluster, TestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
        _ = await Assert.That(second.Options!.InternalListenPort).IsEqualTo(firstPort).Because("Retry certificate generation must reuse the preserved internal port.");

        // The retry releases the reacquired hold for the real bind, so the port is bindable again.
        BindExclusively(firstPort);
    }

    private static ServerPeer[] CreateTwoNodePeers(ClusterIdentity mtls, Uri[] primaries)
    {
        var shared = mtls;
        return ClusterIdentity.CreatePeers([("nodeA", primaries[0]), ("nodeB", primaries[1])], ref shared);
    }

    private static int GetInterNodePort(ServerPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        var interNodeUri = peer.InterNodeUri;
        return interNodeUri == null ? throw new InvalidOperationException("Expected an internode mTLS URL for a multi-node topology.") : interNodeUri.Port;
    }

    private static void BindExclusively(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
        listener.Start();
        listener.Stop();
    }
}
