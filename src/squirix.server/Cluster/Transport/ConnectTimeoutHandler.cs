using System;
using System.IO;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;

namespace Squirix.Server.Cluster.Transport;

/// <summary>Reports a connect to a peer that timed out as the connect failure it is, instead of as a canceled request.</summary>
/// <remarks>
/// <para>
/// <see cref="SocketsHttpHandler" /> fails a request whose connection was not established within its connect timeout, TLS handshake included,
/// with an <see cref="OperationCanceledException" /> whose inner exception is a <see cref="TimeoutException" />. A gRPC client reports that as a
/// canceled call, the same status as a call canceled after its request may have reached the peer. This handler rethrows it as an
/// <see cref="HttpRequestException" /> with <see cref="HttpRequestError.ConnectionError" />, as a refused connect fails, caused by an
/// <see cref="IOException" /> that carries the timeout: the client reports a transport failure as unavailable only when an I/O or socket
/// failure caused it, and keeps the exception as the cause of the status, so a forward recognizes that the attempt never connected.
/// </para>
/// <para>
/// The rewrite applies only when no byte of the request can have been sent. The request token must not be canceled: the per-attempt timeout,
/// the deadline and the pool closing all cancel it, and their cancellation, which may follow a sent request, passes through unchanged, even
/// when a connect timeout raced it. The cause must be a <see cref="TimeoutException" />: below an <see cref="HttpClient" />, whose own timeout
/// is the only other source of that shape and is not used here, the sockets handler raises it only while it establishes a connection, and a
/// connection is handed to a request only once it is established, so nothing was written for this request.
/// </para>
/// </remarks>
internal sealed class ConnectTimeoutHandler : DelegatingHandler
{
    private const string TimedOut = "The connection to the peer was not established within the connect timeout; nothing was sent.";

    /// <summary>Initializes a new instance of the <see cref="ConnectTimeoutHandler" /> class.</summary>
    /// <param name="innerHandler">The handler that sends the requests; disposed with this handler.</param>
    internal ConnectTimeoutHandler(HttpMessageHandler innerHandler)
        : base(innerHandler)
    {
    }

    /// <summary>Bounds the connect of the sockets handler a peer handler sends through, and wraps the peer handler.</summary>
    /// <param name="peerHandler">The peer handler; one that is not, or does not wrap, a <see cref="SocketsHttpHandler" /> keeps its connect as it is.</param>
    /// <param name="connectTimeout">The connect timeout, TLS handshake included.</param>
    /// <param name="owned">Whether the pool created the handler; a factory-supplied handler keeps a finite connect timeout it chose itself.</param>
    /// <param name="dialTimeout">The bound of the dial alone, before any TLS handshake (<see cref="BoundedDial" />); <see langword="null" /> for none.</param>
    /// <returns>The wrapper; disposing it disposes <paramref name="peerHandler" />.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="connectTimeout" /> is zero, negative, or infinite.</exception>
    internal static ConnectTimeoutHandler Wrap(HttpMessageHandler peerHandler, TimeSpan connectTimeout, bool owned, TimeSpan? dialTimeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectTimeout, TimeSpan.Zero);
        var current = peerHandler;
        while (current is DelegatingHandler { InnerHandler: { } next })
            current = next;

        if (current is SocketsHttpHandler socketsHandler)
        {
            if (owned || socketsHandler.ConnectTimeout == Timeout.InfiniteTimeSpan)
                socketsHandler.ConnectTimeout = connectTimeout;

            if (dialTimeout is { } dial)
                BoundedDial.Apply(socketsHandler, dial);
        }

        return new ConnectTimeoutHandler(peerHandler);
    }

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        try
        {
            return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
        }
        catch (OperationCanceledException exception) when (IsConnectTimeout(exception, cancellationToken))
        {
            throw new HttpRequestException(HttpRequestError.ConnectionError, TimedOut, new IOException(TimedOut, exception));
        }
    }

    /// <summary>Determines whether a cancellation is a connect timeout of the sockets handler rather than a cancellation of the request.</summary>
    /// <param name="exception">The cancellation the inner handler raised.</param>
    /// <param name="requestToken">The cancellation token of the request.</param>
    /// <returns><see langword="true" /> when the connect timed out and the request itself was not canceled, so it was never sent.</returns>
    private static bool IsConnectTimeout(OperationCanceledException exception, CancellationToken requestToken) =>
        exception.InnerException is TimeoutException && !requestToken.IsCancellationRequested;
}
