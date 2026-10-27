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
    /// The client's own handler pings its connections with HTTP/2 keepalive whether or not a call is in flight, with a ping timeout longer than
    /// the per-attempt connect timeout, so a node whose host went silent is found while the connection is idle and a late acknowledgement does
    /// not close a healthy one.
    /// </summary>
    [Test]
    public async Task ChannelHandlerPingsIdleConnections()
    {
        using var handler = GrpcTransportEndpoints.CreateChannelHandler();

        var sockets = await Assert.That(handler).IsTypeOf<SocketsHttpHandler>();
        _ = await Assert.That(sockets?.KeepAlivePingPolicy).IsEqualTo(HttpKeepAlivePingPolicy.Always);
        _ = await Assert.That(sockets?.KeepAlivePingDelay).IsEqualTo(GrpcTransportEndpoints.KeepAlivePingDelay);
        _ = await Assert.That(sockets?.KeepAlivePingTimeout).IsEqualTo(GrpcTransportEndpoints.KeepAlivePingTimeout);
        _ = await Assert.That(GrpcTransportEndpoints.KeepAlivePingTimeout).IsGreaterThan(BootstrapConnectOptions.DefaultPerAttemptTimeout);
    }
}
