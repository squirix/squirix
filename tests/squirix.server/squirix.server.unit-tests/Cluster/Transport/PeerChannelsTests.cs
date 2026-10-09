using System;
using System.Collections.Generic;
using System.Diagnostics.Metrics;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>A handler the peer handler factory supplies gets keepalive pings on the forward channel only when it chose none itself.</summary>
public sealed class PeerChannelsTests
{
    private const string Peer = "node-b";

    /// <summary>A factory handler without a keepalive delay is pinged on the forward channel; the lease channel handler is not.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FactoryHandlerGetsKeepAlive(CancellationToken cancellationToken)
    {
        using var forward = new SocketsHttpHandler();
        using var lease = new SocketsHttpHandler();

        await CreatePoolAsync(forward, lease, cancellationToken);

        _ = await Assert.That(forward.KeepAlivePingPolicy).IsEqualTo(HttpKeepAlivePingPolicy.Always);
        _ = await Assert.That(forward.KeepAlivePingDelay).IsNotEqualTo(Timeout.InfiniteTimeSpan);
        _ = await Assert.That(lease.KeepAlivePingDelay).IsEqualTo(Timeout.InfiniteTimeSpan);
    }

    /// <summary>A factory handler that set its own keepalive keeps every part of it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FactoryKeepAliveIsKept(CancellationToken cancellationToken)
    {
        using var forward = new SocketsHttpHandler
        {
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(7),
            KeepAlivePingPolicy = HttpKeepAlivePingPolicy.WithActiveRequests,
        };
        using var lease = new SocketsHttpHandler();

        await CreatePoolAsync(forward, lease, cancellationToken);

        _ = await Assert.That(forward.KeepAlivePingDelay).IsEqualTo(TimeSpan.FromSeconds(30));
        _ = await Assert.That(forward.KeepAlivePingTimeout).IsEqualTo(TimeSpan.FromSeconds(7));
        _ = await Assert.That(forward.KeepAlivePingPolicy).IsEqualTo(HttpKeepAlivePingPolicy.WithActiveRequests);
    }

    /// <summary>Creates and disposes a pool whose factory supplies the forward handler first, then the lease handler.</summary>
    /// <param name="forward">The forward channel handler.</param>
    /// <param name="lease">The lease channel handler.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes once the pool is disposed.</returns>
    private static async Task CreatePoolAsync(SocketsHttpHandler forward, SocketsHttpHandler lease, CancellationToken cancellationToken)
    {
        using var meter = new Meter("test-peer-channels");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var certificate = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var supplied = new Queue<HttpMessageHandler>([forward, lease]);
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = _ => new ServerCallPolicy(new ServerCallPolicyInstrumentation(new ServerCallPolicyMetrics(meter), new ServerRpcTimeoutMetrics(meter))),
            PeerHandlerFactory = _ => supplied.Dequeue(),
            MtlsOptions = new MtlsOptions { InternalListenPort = 6601 },
            Certificate = certificate,
            InterNodeMtlsEnabled = true,
            ForwardConnectTimeout = TimeSpan.FromSeconds(1),
        };

        await using var pool = new ServerClientPool(
            [new ServerPeer { NodeId = Peer, Uri = new Uri("https://localhost:6500") }],
            args,
            new ServerClientPoolMetrics(meter),
            NullLogger<ServerClientPool>.Instance);
    }
}
