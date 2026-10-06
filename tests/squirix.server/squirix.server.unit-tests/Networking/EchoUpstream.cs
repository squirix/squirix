using System;
using System.Collections.Generic;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.UnitTests.Networking;

/// <summary>A loopback listener that echoes every byte it receives and counts them, standing in for a node behind a proxy.</summary>
internal sealed class EchoUpstream : IAsyncDisposable
{
    private readonly Task _acceptLoop;
    private readonly List<Socket> _clients = [];
    private readonly Lock _gate = new();
    private readonly Socket _listener;
    private long _received;

    private EchoUpstream(Socket listener)
    {
        _listener = listener;
        EndPoint = listener.LocalEndPoint is IPEndPoint bound ? bound : throw new InvalidOperationException("The echo listener has no loopback endpoint.");
        _acceptLoop = AcceptLoopAsync();
    }

    internal IPEndPoint EndPoint { get; }

    /// <summary>Gets the number of bytes received from every connection so far.</summary>
    internal long Received => Interlocked.Read(ref _received);

    public async ValueTask DisposeAsync()
    {
        _listener.Dispose();
        lock (_gate)
        {
            for (var i = 0; i < _clients.Count; i++)
                _clients[i].Dispose();

            _clients.Clear();
        }

#pragma warning disable VSTHRD003 // The accept loop is started by this upstream's constructor and ends once the listener above is closed.
        await _acceptLoop;
#pragma warning restore VSTHRD003
    }

    internal static EchoUpstream Start()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen();
        return new EchoUpstream(listener);
    }

    private async Task AcceptLoopAsync()
    {
        var echoes = new List<Task>();
        while (true)
        {
            Socket client;
            try
            {
                client = await _listener.AcceptAsync(CancellationToken.None);
            }
            catch (Exception exception) when (exception is SocketException or ObjectDisposedException)
            {
                break;
            }

            lock (_gate)
                _clients.Add(client);

            echoes.Add(EchoAsync(client));
        }

        await Task.WhenAll(echoes);
    }

    private async Task EchoAsync(Socket client)
    {
        var buffer = new byte[4096];
        try
        {
            while (true)
            {
                var read = await client.ReceiveAsync(buffer.AsMemory(), SocketFlags.None, CancellationToken.None);
                if (read == 0)
                {
                    client.Shutdown(SocketShutdown.Send);
                    return;
                }

                _ = Interlocked.Add(ref _received, read);
                var pending = buffer.AsMemory(0, read);
                while (!pending.IsEmpty)
                {
                    var sent = await client.SendAsync(pending, SocketFlags.None, CancellationToken.None);
                    pending = pending[sent..];
                }
            }
        }
        catch (Exception exception) when (exception is SocketException or ObjectDisposedException)
        {
            // The peer or the proxy went away; the connection is finished either way.
        }
        finally
        {
            client.Dispose();
        }
    }
}
