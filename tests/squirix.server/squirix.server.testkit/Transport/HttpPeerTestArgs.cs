using System;
using System.IO;
using System.Net.Http;
using System.Net.Security;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Transport;

/// <summary>Adapts deterministic HTTP message handlers to the internal peer client seam for tests.</summary>
[Immutable]
internal sealed class HttpPeerTestArgs
{
    /// <summary>
    /// How long a connection of the forward channel may receive nothing before the handler pings the peer, whether or not a call is in flight
    /// (<see cref="HttpKeepAlivePingPolicy.Always" />), so a connection that went silent is found while it is idle, before the next forward is
    /// written to it. It is the shortest delay the handler accepts.
    /// </summary>
    /// <remarks>
    /// The handler checks its connections once per second (a quarter of the shorter of the two bounds, but at least a second), so a ping goes
    /// out up to a tick after the delay and its timeout is noticed up to a tick after it expired: a silent connection is closed after
    /// <see cref="KeepAlivePingDelay" /> plus <see cref="KeepAlivePingTimeout" /> plus up to two ticks, four to five seconds, so a forward
    /// written to it meanwhile usually ends as the 3-second per-attempt timeout. Such a forward, and one failed by the closed connection, are
    /// ambiguous; the first one after the connection is closed dials anew, fails at the dial bound and falls back to another member. An idle
    /// connection costs one ping and its acknowledgement per second.
    /// </remarks>
    internal static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(1);

    /// <summary>How long the handler waits for the acknowledgement of a ping before it closes the connection.</summary>
    /// <remarks>
    /// Two seconds, not the one-second minimum: a ping that is answered late because the peer or this process paused (a garbage collection, a
    /// starved thread pool) does not close a healthy connection, at the price of one more second before a dead one is found.
    /// </remarks>
    internal static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(2);

    /// <summary>Bounds a connect of either channel, TLS handshake included, so a peer that accepts but never answers cannot hold a connection open.</summary>
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    private readonly ServerClientPoolArgs _args;
    private readonly Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler>? _ownedHandlerFactory;
    private readonly Func<string, HttpMessageHandler>? _peerHandlerFactory;

    private HttpPeerTestArgs(
        ServerClientPoolArgs args,
        Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler>? ownedHandlerFactory,
        Func<string, HttpMessageHandler>? peerHandlerFactory)
    {
        _args = args;
        _ownedHandlerFactory = ownedHandlerFactory;
        _peerHandlerFactory = peerHandlerFactory;
    }

    internal static ServerClientPoolArgs WithHandlers(
        ServerClientPoolArgs args,
        Func<MtlsCertificate?, string, TrackedConnections, HttpMessageHandler>? ownedHandlerFactory = null,
        Func<string, HttpMessageHandler>? peerHandlerFactory = null) =>
        args with
        {
            ChannelsOverride = new HttpPeerTestArgs(args, ownedHandlerFactory, peerHandlerFactory).CreateChannels,
        };

    /// <summary>Makes the connections of a handler the pool does not own count in the pool's gate, so disposal drains and aborts them before the material is released.</summary>
    /// <param name="handler">The handler a factory supplied; one that is not, or does not wrap, a <see cref="SocketsHttpHandler" /> stays untracked.</param>
    /// <param name="connections">The pool's tracked connections.</param>
    /// <remarks>An existing connect callback keeps dialing; the tracked callback wraps the stream it returns.</remarks>
    /// <exception cref="InvalidOperationException">The handler's connect callback already tracks the connections of a pool, so a factory returned one handler twice.</exception>
    internal static void TrackFactoryConnections(HttpMessageHandler handler, TrackedConnections connections)
    {
        var current = handler;
        while (current is DelegatingHandler { InnerHandler: { } next })
            current = next;

        if (current is not SocketsHttpHandler socketsHandler)
            return;

        var inner = socketsHandler.ConnectCallback;
        if (inner?.Target is TrackingConnectCallback)
            throw new InvalidOperationException("The peer handler factory returned a handler that already belongs to a pool; it must create a fresh handler per call.");

        socketsHandler.ConnectCallback = new TrackingConnectCallback(connections, inner).ConnectAsync;
    }

    /// <summary>Bounds the connect of the sockets handler a peer handler sends through.</summary>
    /// <param name="peerHandler">The peer handler; one that is not, or does not wrap, a <see cref="SocketsHttpHandler" /> keeps its connect as it is.</param>
    /// <param name="owned">Whether the pool created the handler; a factory-supplied handler keeps a finite connect timeout it chose itself.</param>
    /// <param name="dialTimeout">
    /// The bound of the dial alone, before any TLS handshake (<see cref="BoundedDial" />); <see langword="null" /> for none. Only the forward
    /// channel has one, and only it pings with keepalive; a factory-supplied handler that sets its own keepalive delay keeps its keepalive.
    /// </param>
    /// <param name="timeProvider">The clock the dial bound counts down on.</param>
    private static void BoundConnect(HttpMessageHandler peerHandler, bool owned, TimeSpan? dialTimeout, TimeProvider timeProvider)
    {
        var current = peerHandler;
        while (current is DelegatingHandler { InnerHandler: { } next })
            current = next;

        if (current is not SocketsHttpHandler socketsHandler)
            return;

        if (owned || socketsHandler.ConnectTimeout == Timeout.InfiniteTimeSpan)
            socketsHandler.ConnectTimeout = ConnectTimeout;

        if (dialTimeout is not { } dial)
            return;

        BoundedDial.Apply(socketsHandler, dial, timeProvider);
        if (!owned && socketsHandler.KeepAlivePingDelay != Timeout.InfiniteTimeSpan)
            return;
        socketsHandler.KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always;
        socketsHandler.KeepAlivePingDelay = KeepAlivePingDelay;
        socketsHandler.KeepAlivePingTimeout = KeepAlivePingTimeout;
    }

    private static SocketsHttpHandler CreateTrackedHandler(
        Uri address,
        MtlsCertificate? certificate,
        PeerHandshakeVerifier? verifier,
        string nodeId,
        TrackedConnections connections)
    {
        var handler = new SocketsHttpHandler
        {
            AllowAutoRedirect = false,
            UseProxy = false,
            EnableMultipleHttp2Connections = true,
            ConnectCallback = (context, token) => TrackedConnectionStream.ConnectAsync(connections, context.DnsEndPoint, token),
        };
        try
        {
            if (certificate != null)
            {
                if (!certificate.Enabled || certificate.NodeCertificate == null || certificate.TrustAnchor == null || verifier == null)
                    throw new InvalidOperationException("The peer fixture requires loaded mTLS material.");

                handler.SslOptions = NativePeerClient.CreateTlsOptions(address.IdnHost, nodeId, certificate, verifier);
                handler.SslOptions.ApplicationProtocols = [SslApplicationProtocol.Http2, SslApplicationProtocol.Http11];
            }

            return handler;
        }
        catch
        {
            handler.Dispose();
            throw;
        }
    }

    private PeerChannels CreateChannels(string nodeId, Uri address, PeerHandshakeVerifier? verifier, TrackedConnections connections, TimeSpan forwardConnectTimeout)
    {
        HttpClient? forward = null;
        HttpClient? replication = null;
        try
        {
            forward = CreateClient(address, nodeId, verifier, connections, forwardConnectTimeout);
            replication = CreateClient(address, nodeId, verifier, connections, null);
            var created = new PeerChannels(new PeerClient(forward, _args.RingFingerprint), new PeerClient(replication, null), forward, replication);
            forward = null;
            replication = null;
            return created;
        }
        finally
        {
            forward?.Dispose();
            replication?.Dispose();
        }
    }

    private HttpClient CreateClient(
        Uri address,
        string nodeId,
        PeerHandshakeVerifier? verifier,
        TrackedConnections connections,
        TimeSpan? dialTimeout)
    {
        var factory = _ownedHandlerFactory ?? ((certificate, expectedPeerNodeId, tracked) =>
            CreateTrackedHandler(address, certificate, verifier, expectedPeerNodeId, tracked));
        HttpMessageHandler? ownedHandler = null;
        try
        {
            HttpMessageHandler peerHandler;
            if (_args.InterNodeMtlsEnabled)
            {
                if (_args.Certificate is not { Enabled: true } certificate)
                    throw new InvalidOperationException("Cluster mTLS material must be loaded for internode transport.");

                var factoryHandler = _peerHandlerFactory?.Invoke(nodeId);
                if (factoryHandler != null)
                {
                    TrackFactoryConnections(factoryHandler, connections);
                    peerHandler = factoryHandler;
                }
                else
                {
                    ownedHandler = factory.Invoke(certificate, nodeId, connections);
                    peerHandler = ownedHandler;
                }
            }
            else
            {
                ownedHandler = factory.Invoke(null, nodeId, connections);
                peerHandler = ownedHandler;
            }

            BoundConnect(peerHandler, ownedHandler != null, dialTimeout, _args.TimeProvider);
            var client = new HttpClient(peerHandler, ownedHandler != null) { BaseAddress = address, Timeout = Timeout.InfiniteTimeSpan };
            ownedHandler = null;
            return client;
        }
        finally
        {
            ownedHandler?.Dispose();
        }
    }

    /// <summary>The connect callback the pool installs on a factory-supplied handler; its type marks a handler that already belongs to a pool.</summary>
    [Immutable]
    private sealed class TrackingConnectCallback
    {
        private readonly TrackedConnections _connections;
        private readonly Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? _inner;

        internal TrackingConnectCallback(TrackedConnections connections, Func<SocketsHttpConnectionContext, CancellationToken, ValueTask<Stream>>? inner)
        {
            _connections = connections;
            _inner = inner;
        }

        internal ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken) =>
            _inner == null
                ? TrackedConnectionStream.ConnectAsync(_connections, context.DnsEndPoint, cancellationToken)
                : TrackedWrappedStream.ConnectAsync(_connections, context, _inner, cancellationToken);
    }
}
