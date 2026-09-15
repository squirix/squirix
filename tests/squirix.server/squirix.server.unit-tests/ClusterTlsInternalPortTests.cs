using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
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
public sealed class ClusterTlsInternalPortTests
{
    /// <summary>An allocated internal port must stay bound until released, so a parallel test cannot grab it between probing and Kestrel bind.</summary>
    [Test]
    public async Task InternalPortStaysBoundUntilReleased()
    {
        using var mtls = new ClusterTls();
        var primaries = AllocatePrimaryUris();
        var peers = CreateTwoNodePeers(mtls, primaries);
        var internalPort = GetInterNodePort(peers[0]);

        _ = NodeExceptionAssert.For<SocketException>().Throws(internalPort, static port => BindExclusively(port));

        ClusterTls.ReleaseInternalPort(internalPort);
        BindExclusively(internalPort);

        _ = await Assert.That(GetInterNodePort(peers[1]) != internalPort).IsTrue().Because("Sibling nodes must not share one internal listener port.");
    }

    /// <summary>Parallel cluster owners must receive distinct internal ports that never reuse a primary port.</summary>
    [Test]
    public async Task ClustersGetDistinctInternalPorts()
    {
        const int ownerCount = 8;
        var seen = new HashSet<int>();
        var owners = new ClusterTls[ownerCount];
        try
        {
            for (var i = 0; i < ownerCount; i++)
            {
                owners[i] = new ClusterTls();
                var primaries = AllocatePrimaryUris();
                var peers = CreateTwoNodePeers(owners[i], primaries);
                for (var p = 0; p < peers.Length; p++)
                {
                    var internalPort = GetInterNodePort(peers[p]);
                    _ = await Assert.That(seen.Add(internalPort)).IsTrue().Because($"Internal port {internalPort} was issued twice.");
                    _ = await Assert.That(internalPort != primaries[0].Port && internalPort != primaries[1].Port).IsTrue()
                                    .Because($"Internal port {internalPort} reuses a primary listener port.");
                }
            }
        }
        finally
        {
            for (var i = 0; i < ownerCount; i++)
                owners[i].Dispose();
        }
    }

    private static Uri[] AllocatePrimaryUris() =>
    [
        ListenPortPool.ServerUnitTests.NextHttpUri(),
        ListenPortPool.ServerUnitTests.NextHttpUri(),
    ];

    private static ServerPeer[] CreateTwoNodePeers(ClusterTls mtls, Uri[] primaries)
    {
        var shared = mtls;
        return ClusterTls.CreatePeers([("nodeA", primaries[0]), ("nodeB", primaries[1])], ref shared);
    }

    private static int GetInterNodePort(ServerPeer peer)
    {
        ArgumentNullException.ThrowIfNull(peer);
        var interNodeUri = peer.InterNodeUri;
        return interNodeUri == null ? throw new InvalidOperationException("Expected an inter-node mTLS URL for a multi-node topology.") : interNodeUri.Port;
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
