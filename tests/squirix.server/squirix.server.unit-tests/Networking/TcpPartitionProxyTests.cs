using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Networking;

/// <summary>The partition proxy forwards, holds, resets, and heals loopback connections on explicit signals only.</summary>
public sealed class TcpPartitionProxyTests
{
    private static readonly byte[] Payload = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];

    /// <summary>Bytes cross the proxy in both directions and every direction counts what it forwarded.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task ForwardsBothDirectionsAndCounts(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(client, Payload.Length, cancellationToken);
        await proxy.WaitForForwardedAsync(ProxyDirection.ClientToUpstream, Payload.Length, cancellationToken);
        await proxy.WaitForForwardedAsync(ProxyDirection.UpstreamToClient, Payload.Length, cancellationToken);

        await SequenceAssert.EqualAsync(Payload, echoed);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(Payload.Length);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.UpstreamToClient)).IsEqualTo(Payload.Length);
        _ = await Assert.That(proxy.AcceptedConnections).IsEqualTo(1);
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(1);
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(0);
    }

    /// <summary>A held direction parks its bytes before the upstream sees them and delivers them intact on release.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task HoldParksBytesUntilRelease(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.ClientToUpstream);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);

        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsTrue();
        _ = await Assert.That(echo.Received).IsEqualTo(0);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(0);

        proxy.Release(ProxyDirection.ClientToUpstream);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(client, Payload.Length, cancellationToken);
        await proxy.WaitForForwardedAsync(ProxyDirection.ClientToUpstream, Payload.Length, cancellationToken);

        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        await SequenceAssert.EqualAsync(Payload, echoed);
        _ = await Assert.That(echo.Received).IsEqualTo(Payload.Length);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(Payload.Length);
    }

    /// <summary>A held upstream-to-client direction lets the request reach the upstream but parks the reply until release.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task HoldUpstreamToClientParksReplies(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.UpstreamToClient);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.UpstreamToClient, cancellationToken);
        await proxy.WaitForForwardedAsync(ProxyDirection.ClientToUpstream, Payload.Length, cancellationToken);

        _ = await Assert.That(echo.Received).IsEqualTo(Payload.Length);
        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.UpstreamToClient)).IsEqualTo(0);

        proxy.Release(ProxyDirection.UpstreamToClient);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(client, Payload.Length, cancellationToken);

        await SequenceAssert.EqualAsync(Payload, echoed);
    }

    /// <summary>Healing releases a gate a pump is parked on without aborting the connection.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task HealReleasesParkedPump(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.ClientToUpstream);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);

        proxy.Heal();
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(client, Payload.Length, cancellationToken);

        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(1);
        await SequenceAssert.EqualAsync(Payload, echoed);
    }

    /// <summary>A partition resets the live connections and every new one until the proxy heals.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task PartitionResetsConnectionsUntilHealed(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var live = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await live.SendAsync(Payload, SocketFlags.None, cancellationToken);
        _ = await ProxyTestSockets.ReceiveExactlyAsync(live, Payload.Length, cancellationToken);

        await proxy.PartitionAsync();

        _ = await Assert.That(proxy.IsPartitioned).IsTrue();
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(live, cancellationToken)).IsTrue();

        _ = await Assert.That(await ProxyTestSockets.ConnectIsRefusedAsync(proxy.ListenEndPoint, cancellationToken)).IsTrue();
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(1);
        _ = await Assert.That(proxy.AcceptedConnections).IsEqualTo(1);

        proxy.Heal();
        using var healed = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await healed.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(healed, Payload.Length, cancellationToken);

        _ = await Assert.That(proxy.IsPartitioned).IsFalse();
        await SequenceAssert.EqualAsync(Payload, echoed);
        _ = await Assert.That(proxy.AcceptedConnections).IsEqualTo(2);
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(1);
    }

    /// <summary>A pump parked on a held gate is released by a partition, so the partition completes without a release.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task PartitionAbortsParkedPumps(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.ClientToUpstream);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);

        await proxy.PartitionAsync();

        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(echo.Received).IsEqualTo(0);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(client, cancellationToken)).IsTrue();
    }

    /// <summary>Disposal with bridged connections completes and leaves the clients closed.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task DisposeWithLiveConnectionsCompletes(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var first = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        using var second = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await first.SendAsync(Payload, SocketFlags.None, cancellationToken);
        _ = await ProxyTestSockets.ReceiveExactlyAsync(first, Payload.Length, cancellationToken);
        await proxy.WaitForConnectionAsync(2, cancellationToken);

        await proxy.DisposeAsync();
        await proxy.DisposeAsync();

        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(first, cancellationToken)).IsTrue();
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(second, cancellationToken)).IsTrue();
    }

    /// <summary>A client that resets before it is bridged does not stop the proxy from bridging the next connection.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task ClientResetBeforeBridgingKeepsAccepting(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using (var doomed = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken))
            doomed.LingerState = new LingerOption(true, 0);

        using var next = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await next.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(next, Payload.Length, cancellationToken);

        await SequenceAssert.EqualAsync(Payload, echoed);
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(0);
    }

    /// <summary>A FIN in one direction keeps the other direction forwarding, and the connection closes once both ended.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task HalfCloseKeepsOtherDirectionOpen(CancellationToken cancellationToken)
    {
        using var listener = ProxyTestSockets.Listen();
        await using var proxy = await TcpPartitionProxy.StartAsync(ProxyTestSockets.LocalEndPointOf(listener), cancellationToken);
        using var client = await ProxyTestSockets.ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        await proxy.WaitForConnectionAsync(1, cancellationToken);
        using var upstream = await listener.AcceptAsync(cancellationToken);

        client.Shutdown(SocketShutdown.Send);

        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(upstream, cancellationToken)).IsTrue();
        _ = await upstream.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var received = await ProxyTestSockets.ReceiveExactlyAsync(client, Payload.Length, cancellationToken);

        await SequenceAssert.EqualAsync(Payload, received);
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(1);

        upstream.Shutdown(SocketShutdown.Send);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(client, cancellationToken)).IsTrue();
        await proxy.WaitForActiveConnectionsAsync(0, cancellationToken);

        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(0);
    }

    /// <summary>An unreachable upstream resets the client and counts as a connect failure, not as a partition refusal.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task UpstreamConnectFailureResetsClient(CancellationToken cancellationToken)
    {
        await using var proxy = await TcpPartitionProxy.StartAsync(ProxyTestSockets.ReserveClosedEndPoint(), cancellationToken);
        _ = await Assert.That(await ProxyTestSockets.ConnectIsRefusedAsync(proxy.ListenEndPoint, cancellationToken)).IsTrue();

        _ = await Assert.That(proxy.UpstreamConnectFailures).IsEqualTo(1);
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(0);
        _ = await Assert.That(proxy.AcceptedConnections).IsEqualTo(0);
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
    }
}
