using System;
using System.Buffers;
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
        using var client = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ReceiveExactlyAsync(client, Payload.Length, cancellationToken);
        await proxy.WaitForConnectionAsync(1, cancellationToken);

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
        using var client = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.ClientToUpstream);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);

        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsTrue();
        _ = await Assert.That(echo.Received).IsEqualTo(0);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(0);

        proxy.Release(ProxyDirection.ClientToUpstream);
        var echoed = await ReceiveExactlyAsync(client, Payload.Length, cancellationToken);

        _ = await Assert.That(proxy.IsHeld(ProxyDirection.ClientToUpstream)).IsFalse();
        await SequenceAssert.EqualAsync(Payload, echoed);
        _ = await Assert.That(echo.Received).IsEqualTo(Payload.Length);
        _ = await Assert.That(proxy.BytesForwarded(ProxyDirection.ClientToUpstream)).IsEqualTo(Payload.Length);
    }

    /// <summary>A partition resets the live connections and every new one until the proxy heals.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task PartitionResetsConnectionsUntilHealed(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        await using var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var live = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await live.SendAsync(Payload, SocketFlags.None, cancellationToken);
        _ = await ReceiveExactlyAsync(live, Payload.Length, cancellationToken);

        await proxy.PartitionAsync();

        _ = await Assert.That(proxy.IsPartitioned).IsTrue();
        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await IsClosedAsync(live, cancellationToken)).IsTrue();

        using var refused = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await Assert.That(await IsRefusedAsync(refused, cancellationToken)).IsTrue();
        _ = await Assert.That(proxy.RefusedConnections).IsEqualTo(1);
        _ = await Assert.That(proxy.AcceptedConnections).IsEqualTo(1);

        proxy.Heal();
        using var healed = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await healed.SendAsync(Payload, SocketFlags.None, cancellationToken);
        var echoed = await ReceiveExactlyAsync(healed, Payload.Length, cancellationToken);

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
        using var client = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);

        proxy.Hold(ProxyDirection.ClientToUpstream);
        _ = await client.SendAsync(Payload, SocketFlags.None, cancellationToken);
        await proxy.WaitUntilHeldAsync(ProxyDirection.ClientToUpstream, cancellationToken);

        await proxy.PartitionAsync();

        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(echo.Received).IsEqualTo(0);
        _ = await Assert.That(await IsClosedAsync(client, cancellationToken)).IsTrue();
    }

    /// <summary>Disposal with bridged connections completes and leaves the clients closed.</summary>
    /// <param name="cancellationToken">The test cancellation token; bounds the waits only.</param>
    [Test]
    public async Task DisposeWithLiveConnectionsCompletes(CancellationToken cancellationToken)
    {
        await using var echo = EchoUpstream.Start();
        var proxy = await TcpPartitionProxy.StartAsync(echo.EndPoint, cancellationToken);
        using var first = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        using var second = await ConnectAsync(proxy.ListenEndPoint, cancellationToken);
        _ = await first.SendAsync(Payload, SocketFlags.None, cancellationToken);
        _ = await ReceiveExactlyAsync(first, Payload.Length, cancellationToken);
        await proxy.WaitForConnectionAsync(2, cancellationToken);

        await proxy.DisposeAsync();
        await proxy.DisposeAsync();

        _ = await Assert.That(proxy.ActiveConnections).IsEqualTo(0);
        _ = await Assert.That(await IsClosedAsync(first, cancellationToken)).IsTrue();
        _ = await Assert.That(await IsClosedAsync(second, cancellationToken)).IsTrue();
    }

    internal static async Task<Socket> ConnectAsync(IPEndPoint endPoint, CancellationToken cancellationToken)
    {
        var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(endPoint, cancellationToken);
            return socket;
        }
        catch
        {
            socket.Dispose();
            throw;
        }
    }

    /// <summary>Tells whether the peer closed or reset the connection instead of sending more bytes.</summary>
    /// <param name="socket">The connected socket to read from.</param>
    /// <param name="cancellationToken">Bounds the read.</param>
    /// <returns><see langword="true" /> when the read ends with a FIN or a reset.</returns>
    internal static async Task<bool> IsClosedAsync(Socket socket, CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(1);
        try
        {
            return await socket.ReceiveAsync(buffer.AsMemory(0, 1), SocketFlags.None, cancellationToken) == 0;
        }
        catch (SocketException)
        {
            return true;
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    /// <summary>Tells whether a freshly connected socket is reset by the proxy instead of being served.</summary>
    /// <param name="socket">The connected socket to probe.</param>
    /// <param name="cancellationToken">Bounds the probe.</param>
    /// <returns><see langword="true" /> when the send or the following read fails or ends without an echo.</returns>
    internal static async Task<bool> IsRefusedAsync(Socket socket, CancellationToken cancellationToken)
    {
        try
        {
            _ = await socket.SendAsync(Payload, SocketFlags.None, cancellationToken);
        }
        catch (SocketException)
        {
            return true;
        }

        return await IsClosedAsync(socket, cancellationToken);
    }

    internal static async Task<byte[]> ReceiveExactlyAsync(Socket socket, int count, CancellationToken cancellationToken)
    {
        var buffer = new byte[count];
        var filled = 0;
        while (filled < count)
        {
            var read = await socket.ReceiveAsync(buffer.AsMemory(filled), SocketFlags.None, cancellationToken);
            if (read == 0)
                throw new InvalidOperationException($"The connection closed after {filled} of {count} bytes.");

            filled += read;
        }

        return buffer;
    }
}
