using System;
using System.IO;
using System.Net.Http;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Bounds the dial of a connection, the part before any TLS handshake, and reports a dial that timed out as a failed connect.</summary>
/// <remarks>
/// <para>
/// A host that is down or drops connection attempts never completes the dial, while a live host completes it at once even when it is too
/// loaded to finish a TLS handshake quickly. Bounding only the dial therefore fails the connect to a dead host fast without aborting the slow
/// but healthy handshakes of a loaded one, which the handler's own connect timeout keeps bounding.
/// </para>
/// <para>
/// A dial that timed out returned no stream, so nothing was sent. It fails with an <see cref="IOException" /> that carries a
/// <see cref="TimeoutException" />; the sockets handler reports it as an <see cref="HttpRequestException" /> with
/// <see cref="HttpRequestError.ConnectionError" />, which a gRPC client reports as unavailable with that cause. A cancellation of the connect
/// itself passes through unchanged.
/// </para>
/// </remarks>
[Immutable]
internal sealed class BoundedDial
{
    private const string TimedOut = "The connection to the peer was not established within the dial timeout; nothing was sent.";

    private readonly Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? _inner;
    private readonly TimeSpan _timeout;

    private BoundedDial(Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? inner, TimeSpan timeout)
    {
        _inner = inner;
        _timeout = timeout;
    }

    /// <summary>Bounds the dial of <paramref name="handler" /> by <paramref name="timeout" />, keeping its connect callback as the dial.</summary>
    /// <param name="handler">A handler that has not started.</param>
    /// <param name="timeout">The dial timeout.</param>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="timeout" /> is zero, negative, or infinite.</exception>
    internal static void Apply(SocketsHttpHandler handler, TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        handler.ConnectCallback = new BoundedDial(handler.ConnectCallback, timeout).ConnectAsync;
    }

    /// <summary>Opens a TCP connection to the endpoint of <paramref name="context" />, as the sockets handler does without a connect callback.</summary>
    /// <param name="context">The connection context.</param>
    /// <param name="cancellationToken">The dial token.</param>
    /// <returns>The connected stream.</returns>
    private static async ValueTask<Stream> DialAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        Socket? socket = new(SocketType.Stream, ProtocolType.Tcp) { NoDelay = true };
        try
        {
            await socket.ConnectAsync(context.DnsEndPoint, cancellationToken).ConfigureAwait(false);
            var stream = new NetworkStream(socket, true);
            socket = null;
            return stream;
        }
        finally
        {
            socket?.Dispose();
        }
    }

    private async ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken)
    {
        using var bounded = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        bounded.CancelAfter(_timeout);
        try
        {
            return _inner == null
                ? await DialAsync(context, bounded.Token).ConfigureAwait(false)
                : await _inner(context, bounded.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (!cancellationToken.IsCancellationRequested && bounded.IsCancellationRequested)
        {
            throw new IOException(TimedOut, new TimeoutException(TimedOut, exception));
        }
    }
}
