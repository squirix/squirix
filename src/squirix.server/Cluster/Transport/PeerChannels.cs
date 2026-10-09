using System;
using System.Net.Http;
using System.Threading;
using Grpc.Net.Client;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Threading;

namespace Squirix.Server.Cluster.Transport;

/// <summary>The two channels of one peer: one for client calls forwarded to the peer, one for every other internode call.</summary>
/// <remarks>
/// <para>
/// A forward bounds its dial with the short forward connect timeout (<see cref="BoundedDial" />), so a peer whose host is down fails the
/// connect, which sent nothing, well before the forward's per-attempt timeout, and another member may take the call. Both channels bound the
/// whole connect, TLS handshake included, by a long timeout: a short one aborts the slow but healthy handshakes of a loaded host. Replication,
/// elections and probes never get the short dial bound: they retry on their own schedule.
/// </para>
/// <para>
/// The forward channel also pings its connections with HTTP/2 keepalive (<see cref="KeepAlivePingDelay" />), so a peer whose host went silent
/// without closing the connection is detected while the connection is idle and the next forward dials anew instead of waiting out its
/// per-attempt timeout on the dead one. The lease channel sends no pings.
/// </para>
/// <para>
/// Both channels get their own handler, created the same way: from the peer handler factory under mTLS when it supplies one, else owned by the
/// pool. Both count their connections in the pool's tracked connections.
/// </para>
/// </remarks>
[Immutable]
internal sealed class PeerChannels
{
    /// <summary>Bounds a connect of either channel, TLS handshake included, so a peer that accepts but never answers cannot hold a connection open.</summary>
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

    /// <summary>
    /// How long a connection of the forward channel may receive nothing before the handler pings the peer, whether or not a call is in flight
    /// (<see cref="HttpKeepAlivePingPolicy.Always" />), so a connection that went silent is found while it is idle, before the next forward is
    /// written to it. It is the shortest delay the handler accepts.
    /// </summary>
    /// <remarks>
    /// The handler checks its connections every half of the shorter of the two bounds, so a dead connection is closed after at most about
    /// <see cref="KeepAlivePingDelay" /> plus <see cref="KeepAlivePingTimeout" /> plus that half: about two seconds, three at worst, against
    /// the 3-second per-attempt timeout of a forward. A forward written in that window ends as the attempt timeout or as the failure of the
    /// closed connection, both ambiguous; the next one dials anew, fails at the dial bound and falls back to another member. An idle
    /// connection costs one ping and its acknowledgement per second.
    /// </remarks>
    private static readonly TimeSpan KeepAlivePingDelay = TimeSpan.FromSeconds(1);

    /// <summary>How long the handler waits for the acknowledgement of a ping before it closes the connection; the shortest timeout the handler accepts.</summary>
    private static readonly TimeSpan KeepAlivePingTimeout = TimeSpan.FromSeconds(1);

    private PeerChannels(GrpcChannel forward, GrpcChannel lease)
    {
        Forward = forward;
        Lease = lease;
    }

    /// <summary>Gets the channel of client calls forwarded to the peer.</summary>
    internal GrpcChannel Forward { get; }

    /// <summary>Gets the channel of every other internode call: replication, elections and probes.</summary>
    internal GrpcChannel Lease { get; }

    /// <summary>Creates both channels of a peer; when the second fails, the first is disposed before the failure propagates.</summary>
    /// <param name="nodeId">The peer node identifier.</param>
    /// <param name="address">The address both channels dial.</param>
    /// <param name="args">The pool arguments.</param>
    /// <param name="connections">The pool's tracked connections.</param>
    /// <param name="forwardConnectTimeout">The dial timeout of the forward channel.</param>
    /// <returns>The channels.</returns>
    internal static PeerChannels Create(string nodeId, Uri address, ServerClientPoolArgs args, TrackedConnections connections, TimeSpan forwardConnectTimeout)
    {
        GrpcChannel? forward = null;
        try
        {
            forward = GrpcChannel.ForAddress(address, CreateChannelOptions(nodeId, args, connections, forwardConnectTimeout));
            var created = new PeerChannels(forward, GrpcChannel.ForAddress(address, CreateChannelOptions(nodeId, args, connections, null)));
            forward = null;
            return created;
        }
        finally
        {
            forward?.Dispose();
        }
    }

    /// <summary>Disposes both channels; a failure of the first does not leave the second undisposed.</summary>
    /// <returns>The first failure, or <see langword="null" /> when both channels were disposed.</returns>
    internal Exception? Close()
    {
        var forward = Isolated.Run(Forward, static channel => channel.Dispose());
        var lease = Isolated.Run(Lease, static channel => channel.Dispose());
        return forward ?? lease;
    }

    /// <summary>Bounds the connect of the sockets handler a peer handler sends through.</summary>
    /// <param name="peerHandler">The peer handler; one that is not, or does not wrap, a <see cref="SocketsHttpHandler" /> keeps its connect as it is.</param>
    /// <param name="owned">Whether the pool created the handler; a factory-supplied handler keeps a finite connect timeout it chose itself.</param>
    /// <param name="dialTimeout">
    /// The bound of the dial alone, before any TLS handshake (<see cref="BoundedDial" />); <see langword="null" /> for none. Only the forward
    /// channel has one, and only it pings with keepalive; a factory-supplied handler that sets its own keepalive delay keeps its keepalive.
    /// </param>
    private static void BoundConnect(HttpMessageHandler peerHandler, bool owned, TimeSpan? dialTimeout)
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

        BoundedDial.Apply(socketsHandler, dial);
        if (owned || socketsHandler.KeepAlivePingDelay == Timeout.InfiniteTimeSpan)
        {
            socketsHandler.KeepAlivePingPolicy = HttpKeepAlivePingPolicy.Always;
            socketsHandler.KeepAlivePingDelay = KeepAlivePingDelay;
            socketsHandler.KeepAlivePingTimeout = KeepAlivePingTimeout;
        }
    }

    private static GrpcChannelOptions CreateChannelOptions(string nodeId, ServerClientPoolArgs args, TrackedConnections connections, TimeSpan? dialTimeout)
    {
        var ownedHandlerFactory = args.OwnedHandlerFactory ?? ServerClientPool.ServerGrpcEndpoints.CreateOwnedHandler;
        HttpMessageHandler? ownedHandler = null;
        try
        {
            HttpMessageHandler peerHandler;
            if (args.InterNodeMtlsEnabled)
            {
                if (args.Certificate is not { Enabled: true } certificate)
                    throw new InvalidOperationException("Cluster mTLS material must be loaded for internode transport.");

                var factoryHandler = args.PeerHandlerFactory?.Invoke(nodeId);
                if (factoryHandler != null)
                {
                    ServerClientPool.TrackFactoryConnections(factoryHandler, connections);
                    peerHandler = factoryHandler;
                }
                else
                {
                    ownedHandler = ownedHandlerFactory.Invoke(certificate, nodeId, connections);
                    peerHandler = ownedHandler;
                }
            }
            else
            {
                ownedHandler = ownedHandlerFactory.Invoke(null, nodeId, connections);
                peerHandler = ownedHandler;
            }

            BoundConnect(peerHandler, ownedHandler != null, dialTimeout);
            var options = new GrpcChannelOptions
            {
                HttpHandler = peerHandler,

                // The channel disposes the handler the pool created; a peerHandlerFactory handler stays owned by the factory's
                // caller. The certificates an owned handler presents stay loaded through the pool's material hold until its connections are gone.
                DisposeHttpClient = ownedHandler != null,
                MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
                MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
            };
            ownedHandler = null;
            return options;
        }
        finally
        {
            ownedHandler?.Dispose();
        }
    }
}
