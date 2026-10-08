using System;
using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Diagnostics;

namespace Squirix.Server.UnitTests.Networking;

/// <summary>Raw loopback socket helpers shared by the partition proxy and fabric tests.</summary>
internal static class ProxyTestSockets
{
    private static readonly byte[] Probe = [0x50];

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

    /// <summary>Connects and tells whether the proxy refuses the connection, whether the reset lands during the connect or after it.</summary>
    /// <param name="endPoint">The proxy endpoint to dial.</param>
    /// <param name="cancellationToken">Bounds the connect and the probe.</param>
    /// <returns><see langword="true" /> when the connect fails with a reset or refusal, or the connection is reset without an echo.</returns>
    internal static async Task<bool> ConnectIsRefusedAsync(IPEndPoint endPoint, CancellationToken cancellationToken)
    {
        try
        {
            using var socket = await ConnectAsync(endPoint, cancellationToken);
            return await IsRefusedAsync(socket, cancellationToken);
        }
        catch (SocketException)
        {
            return true;
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
            _ = await socket.SendAsync(Probe, SocketFlags.None, cancellationToken);
        }
        catch (SocketException)
        {
            return true;
        }

        return await IsClosedAsync(socket, cancellationToken);
    }

    /// <summary>Binds and listens on an ephemeral loopback port.</summary>
    /// <returns>The listening socket; the caller disposes it.</returns>
    internal static Socket Listen()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        try
        {
            listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            listener.Listen();
            return listener;
        }
        catch
        {
            listener.Dispose();
            throw;
        }
    }

    /// <summary>Returns the loopback endpoint a bound socket is bound to.</summary>
    /// <param name="socket">A bound loopback socket.</param>
    /// <returns>The bound endpoint.</returns>
    /// <exception cref="InvalidOperationException">Thrown when the socket is not bound to an IP endpoint.</exception>
    internal static IPEndPoint LocalEndPointOf(Socket socket) => KitThrowHelper.Required(socket.LocalEndPoint as IPEndPoint, "The socket is not bound to an IP endpoint.");

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

    /// <summary>Returns a loopback endpoint nothing listens on, by binding an ephemeral port and releasing it.</summary>
    /// <returns>An endpoint whose connects are refused.</returns>
    internal static IPEndPoint ReserveClosedEndPoint()
    {
        using var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        return LocalEndPointOf(probe);
    }
}
