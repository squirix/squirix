using System;
using System.Net.Http;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal.Cluster.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the HTTP handler the client creates for its own channels.</summary>
[Immutable]
public sealed class GrpcTransportEndpointsTests
{
    /// <summary>
    /// The client's own handler pings its connections with HTTP/2 keepalive whether or not a call is in flight, with the delay and timeout of
    /// the server's forward channel, so a node whose host went silent is found while the connection is idle.
    /// </summary>
    [Test]
    public async Task ChannelHandlerPingsIdleConnections()
    {
        using var handler = GrpcTransportEndpoints.CreateChannelHandler();

        var sockets = await Assert.That(handler).IsTypeOf<SocketsHttpHandler>();
        _ = await Assert.That(sockets?.KeepAlivePingPolicy).IsEqualTo(HttpKeepAlivePingPolicy.Always);
        _ = await Assert.That(sockets?.KeepAlivePingDelay).IsEqualTo(TimeSpan.FromSeconds(1));
        _ = await Assert.That(sockets?.KeepAlivePingTimeout).IsEqualTo(TimeSpan.FromSeconds(2));
    }

    /// <summary>
    /// The client's own handler bounds a connect, TLS handshake included, below the per-attempt timeout, and sets no connect callback: with
    /// one the channel no longer manages its connections and refuses to connect.
    /// </summary>
    [Test]
    public async Task ChannelHandlerBoundsConnect()
    {
        using var handler = GrpcTransportEndpoints.CreateChannelHandler();

        var sockets = await Assert.That(handler).IsTypeOf<SocketsHttpHandler>();
        _ = await Assert.That(sockets?.ConnectTimeout).IsEqualTo(TimeSpan.FromSeconds(2));
        _ = await Assert.That(sockets?.ConnectCallback is null).IsTrue();
    }
}
