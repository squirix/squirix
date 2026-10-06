using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Networking;

/// <summary>The fabric maps node-level partition and hold requests onto its directed proxies.</summary>
public sealed class PartitionFabricTests
{
    private static readonly byte[] Payload = [42, 43, 44, 45];

    /// <summary>Isolating a node resets both orientations of every link it has and leaves the other links forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task IsolateAsyncPartitionsEveryLinkOfTheNode(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var fabric = await CreateTriangleAsync(echo.EndPoint, cancellationToken);
        using var live = await ProxyTestSockets.ConnectAsync(fabric["a", "c"].ListenEndPoint, cancellationToken);
        _ = await live.SendAsync(Payload, SocketFlags.None, cancellationToken);
        _ = await ProxyTestSockets.ReceiveExactlyAsync(live, Payload.Length, cancellationToken);

        await fabric.IsolateAsync("c");

        _ = await Assert.That(fabric["a", "c"].IsPartitioned).IsTrue();
        _ = await Assert.That(fabric["c", "a"].IsPartitioned).IsTrue();
        _ = await Assert.That(fabric["b", "c"].IsPartitioned).IsTrue();
        _ = await Assert.That(fabric["c", "b"].IsPartitioned).IsTrue();
        _ = await Assert.That(fabric["a", "b"].IsPartitioned).IsFalse();
        _ = await Assert.That(fabric["b", "a"].IsPartitioned).IsFalse();
        _ = await Assert.That(fabric["a", "c"].ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(live, cancellationToken)).IsTrue();

        _ = await Assert.That(await ProxyTestSockets.ConnectIsRefusedAsync(fabric["c", "a"].ListenEndPoint, cancellationToken)).IsTrue();

        using var served = await ProxyTestSockets.ConnectAsync(fabric["a", "b"].ListenEndPoint, cancellationToken);
        _ = await served.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ProxyTestSockets.ReceiveExactlyAsync(served, Payload.Length, cancellationToken);
        await SequenceAssert.EqualAsync(Payload, echoed);

        fabric.HealAll();
        _ = await Assert.That(fabric["a", "c"].IsPartitioned).IsFalse();
        _ = await Assert.That(fabric["c", "b"].IsPartitioned).IsFalse();
    }

    /// <summary>Holding one direction between two nodes holds exactly the two gates that carry it, whichever side dialed.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task HoldDirectionHoldsOnlyTheMatchingGates(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var fabric = await CreateTriangleAsync(echo.EndPoint, cancellationToken);

        using var ab = await ProxyTestSockets.ConnectAsync(fabric["a", "b"].ListenEndPoint, cancellationToken);
        using var ba = await ProxyTestSockets.ConnectAsync(fabric["b", "a"].ListenEndPoint, cancellationToken);
        using var ac = await ProxyTestSockets.ConnectAsync(fabric["a", "c"].ListenEndPoint, cancellationToken);
        using var ca = await ProxyTestSockets.ConnectAsync(fabric["c", "a"].ListenEndPoint, cancellationToken);
        using var bc = await ProxyTestSockets.ConnectAsync(fabric["b", "c"].ListenEndPoint, cancellationToken);
        using var cb = await ProxyTestSockets.ConnectAsync(fabric["c", "b"].ListenEndPoint, cancellationToken);

        fabric.HoldDirection("a", "b");

        _ = await Assert.That(fabric["a", "b"].IsHeld(ProxyDirection.ClientToUpstream)).IsTrue();
        _ = await Assert.That(fabric["b", "a"].IsHeld(ProxyDirection.UpstreamToClient)).IsTrue();
        _ = await Assert.That(fabric["a", "b"].IsHeld(ProxyDirection.UpstreamToClient)).IsFalse();
        _ = await Assert.That(fabric["b", "a"].IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        _ = await Assert.That(fabric["a", "c"].IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        _ = await Assert.That(fabric["c", "a"].IsHeld(ProxyDirection.UpstreamToClient)).IsFalse();

        foreach (var socket in new[] { ab, ba, ac, ca, bc, cb })
            _ = await socket.SendAsync(Payload, SocketFlags.None, cancellationToken);

        await fabric["a", "b"].WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);
        await fabric["b", "a"].WaitUntilHeldAsync(ProxyDirection.UpstreamToClient, cancellationToken);
        foreach (var socket in new[] { ac, ca, bc, cb })
        {
            var echoed = await ProxyTestSockets.ReceiveExactlyAsync(socket, Payload.Length, cancellationToken);
            await SequenceAssert.EqualAsync(Payload, echoed);
        }

        _ = await Assert.That(fabric["a", "b"].BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(0);
        await fabric["b", "a"].WaitForForwardedAsync(ProxyDirection.ClientToUpstream, Payload.Length, cancellationToken);
        _ = await Assert.That(fabric["b", "a"].BytesForwarded(ProxyDirection.UpstreamToClient)).IsEqualTo(0);

        fabric.ReleaseDirection("a", "b");

        _ = await Assert.That(fabric["a", "b"].IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        _ = await Assert.That(fabric["b", "a"].IsHeld(ProxyDirection.UpstreamToClient)).IsFalse();
        foreach (var socket in new[] { ab, ba })
        {
            var echoed = await ProxyTestSockets.ReceiveExactlyAsync(socket, Payload.Length, cancellationToken);
            await SequenceAssert.EqualAsync(Payload, echoed);
        }
    }

    /// <summary>Disposing the fabric disposes every proxy, so a link that was started is closed afterwards.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task DisposeAsyncDisposesEveryProxy(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        var fabric = await CreateTriangleAsync(echo.EndPoint, cancellationToken);
        var first = fabric["a", "b"];
        var last = fabric["c", "b"];
        using var live = await ProxyTestSockets.ConnectAsync(last.ListenEndPoint, cancellationToken);
        await last.WaitForConnectionAsync(1, cancellationToken);

        await fabric.DisposeAsync();

        _ = await Assert.That(first.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(last.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await ProxyTestSockets.IsClosedAsync(live, cancellationToken)).IsTrue();
    }

    /// <summary>A pair keeps one proxy across repeated starts, refuses a different upstream, and unknown pairs fail loudly.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task EnsureProxyAsyncIsIdempotentPerPair(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var fabric = new PartitionFabric();
        var first = await fabric.EnsureProxyAsync("a", "b", echo.EndPoint, cancellationToken);
        var second = await fabric.EnsureProxyAsync("a", "b", echo.EndPoint, cancellationToken);

        _ = await Assert.That(second).IsSameReferenceAs(first);
        _ = await Assert.That(fabric["a", "b"]).IsSameReferenceAs(first);

        var other = new IPEndPoint(IPAddress.Loopback, echo.EndPoint.Port == 1 ? 2 : 1);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(fabric, other, cancellationToken, static (f, o, ct) => _ = f.EnsureProxyAsync("a", "b", o, ct));
        _ = NodeExceptionAssert.For<KeyNotFoundException>().Throws(fabric, static f => _ = f["b", "a"]);
    }

    private static async Task<PartitionFabric> CreateTriangleAsync(IPEndPoint upstream, CancellationToken cancellationToken)
    {
        var fabric = new PartitionFabric();
        try
        {
            _ = await fabric.EnsureProxyAsync("a", "b", upstream, cancellationToken);
            _ = await fabric.EnsureProxyAsync("b", "a", upstream, cancellationToken);
            _ = await fabric.EnsureProxyAsync("a", "c", upstream, cancellationToken);
            _ = await fabric.EnsureProxyAsync("c", "a", upstream, cancellationToken);
            _ = await fabric.EnsureProxyAsync("b", "c", upstream, cancellationToken);
            _ = await fabric.EnsureProxyAsync("c", "b", upstream, cancellationToken);
            return fabric;
        }
        catch
        {
            await fabric.DisposeAsync();
            throw;
        }
    }
}
