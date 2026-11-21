using System;
using System.IO;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Threading;
using System.Threading.Channels;
using System.Threading.Tasks;
using Squirix.Server.Logging;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Accepts bounded TLS sessions and waits for admitted work before releasing the listener.</summary>
internal sealed class NativeTlsListener
{
    private readonly Socket _listener;
    private readonly Func<EndPoint, SslServerAuthenticationOptions> _tls;
    private readonly Func<SslStream, IPEndPoint, CancellationToken, CancellationToken, Task> _serve;
    private readonly IServerLogger _log;
    private readonly int _capacity;

    internal NativeTlsListener(
        Socket listener,
        SslServerAuthenticationOptions tls,
        Func<SslStream, IPEndPoint, CancellationToken, CancellationToken, Task> serve,
        IServerLogger log,
        int capacity)
        : this(listener, _ => tls, serve, log, capacity)
    {
    }

    internal NativeTlsListener(
        Socket listener,
        Func<EndPoint, SslServerAuthenticationOptions> tls,
        Func<SslStream, IPEndPoint, CancellationToken, CancellationToken, Task> serve,
        IServerLogger log,
        int capacity)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(capacity, 1);
        _listener = listener;
        _tls = tls;
        _serve = serve;
        _log = log;
        _capacity = capacity;
    }

    internal async Task RunAsync(CancellationToken stopAccepting, CancellationToken abort)
    {
        using var accepting = CancellationTokenSource.CreateLinkedTokenSource(stopAccepting, abort);
        using var connections = CancellationTokenSource.CreateLinkedTokenSource(abort);
        var slots = Channel.CreateBounded<int>(new BoundedChannelOptions(_capacity) { SingleReader = true });
        var active = new Task[_capacity];
        Array.Fill(active, Task.CompletedTask);
        for (var i = 0; i < active.Length; i++)
            _ = slots.Writer.TryWrite(i);
        try
        {
            while (true)
            {
                var slot = await slots.Reader.ReadAsync(accepting.Token).ConfigureAwait(false);
                Socket socket;
                try
                {
                    socket = await _listener.AcceptAsync(accepting.Token).ConfigureAwait(false);
                }
                catch (SocketException error) when (error.SocketErrorCode is SocketError.ConnectionAborted or SocketError.ConnectionReset)
                {
                    // A queued connection can reset before accept completes, particularly on Windows.
                    _ = slots.Writer.TryWrite(slot);
                    continue;
                }
#pragma warning disable CA2025 // The finally block awaits every occupied slot before disposing the lifetime sources.
                active[slot] = ServeAsync(socket, slots.Writer, slot, accepting.Token, connections.Token);
#pragma warning restore CA2025
            }
        }
        catch (OperationCanceledException) when (accepting.IsCancellationRequested)
        {
            // Accepted operations keep their independent abort token while draining.
        }
        catch
        {
            await connections.CancelAsync().ConfigureAwait(false);
            throw;
        }
        finally
        {
            _listener.Dispose();
            await Task.WhenAll(active).ConfigureAwait(false);
        }
    }

    private static void LogConnectionFailure(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Debug))
            Events.LogConnectionFailureEvent.Write(logger, ServerLogLevel.Debug, exception, ValueTuple.Create());
    }

    private static void LogSessionFailure(IServerLogger logger, Exception exception)
    {
        if (logger.IsEnabled(ServerLogLevel.Error))
            Events.LogSessionFailureEvent.Write(logger, ServerLogLevel.Error, exception, ValueTuple.Create());
    }

    private async Task ServeAsync(Socket socket, ChannelWriter<int> slots, int slot, CancellationToken graceful, CancellationToken abort)
    {
        try
        {
            using var owner = socket;
            socket.NoDelay = true;
            if (socket.RemoteEndPoint is not IPEndPoint remote)
                return;
            var network = new NetworkStream(socket, false);
            await using var networkLifetime = network.ConfigureAwait(false);
            var tls = new SslStream(network, true);
            await using var tlsLifetime = tls.ConfigureAwait(false);
            await using var interrupt = abort.UnsafeRegister(static state => (state as Socket)?.Dispose(), socket).ConfigureAwait(false);
            using (var handshake = CancellationTokenSource.CreateLinkedTokenSource(graceful, abort))
            {
                handshake.CancelAfter(TimeSpan.FromSeconds(5));
                await tls.AuthenticateAsServerAsync(_tls(remote), handshake.Token).ConfigureAwait(false);
            }

            await _serve(tls, remote, graceful, abort).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is IOException or SocketException or AuthenticationException or OperationCanceledException || (abort.IsCancellationRequested && exception is ObjectDisposedException))
        {
            LogConnectionFailure(_log, exception);
        }
#pragma warning disable CA1031 // Isolate an unexpected session failure from other connections and the accept loop.
        catch (Exception exception)
#pragma warning restore CA1031
        {
            LogSessionFailure(_log, exception);
        }
        finally
        {
            _ = slots.TryWrite(slot);
        }
    }

    private static class Events
    {
        internal static readonly LogEvent LogConnectionFailureEvent = new(3033, nameof(NativeTlsListener.LogConnectionFailure), "TLS connection ended before completing its session");

        internal static readonly LogEvent LogSessionFailureEvent = new(3034, nameof(NativeTlsListener.LogSessionFailure), "TLS session handler failed");
    }
}
