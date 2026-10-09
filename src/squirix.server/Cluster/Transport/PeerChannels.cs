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
/// Both channels get their own handler, created the same way: from the peer handler factory under mTLS when it supplies one, else owned by the
/// pool. Both count their connections in the pool's tracked connections.
/// </para>
/// </remarks>
[Immutable]
internal sealed class PeerChannels
{
    /// <summary>Bounds a connect of either channel, TLS handshake included, so a peer that accepts but never answers cannot hold a connection open.</summary>
    private static readonly TimeSpan ConnectTimeout = TimeSpan.FromSeconds(5);

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
    /// <param name="dialTimeout">The bound of the dial alone, before any TLS handshake (<see cref="BoundedDial" />); <see langword="null" /> for none.</param>
    private static void BoundConnect(HttpMessageHandler peerHandler, bool owned, TimeSpan? dialTimeout)
    {
        var current = peerHandler;
        while (current is DelegatingHandler { InnerHandler: { } next })
            current = next;

        if (current is not SocketsHttpHandler socketsHandler)
            return;

        if (owned || socketsHandler.ConnectTimeout == Timeout.InfiniteTimeSpan)
            socketsHandler.ConnectTimeout = ConnectTimeout;

        if (dialTimeout is { } dial)
            BoundedDial.Apply(socketsHandler, dial);
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
