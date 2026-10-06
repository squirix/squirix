using System;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Adapters.Grpc.Replication;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc.Replication;

/// <summary>Pool disposal under a replication call to a real loopback peer that accepts the TLS connection and never answers the handshake.</summary>
[Immutable]
public sealed class ServerClientPoolTlsShutdownTests : ServerUnitTestBase
{
    private const string LocalNodeId = "node-a";

    private const string PeerNodeId = "node-b";

    /// <summary>Bounds the waits on the stub peer; disposal aborts the stalled connection at once.</summary>
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Disposal returns only after the stalled handshake's connection is closed, and releases the pool's hold on the material only then, so the
    /// certificates are never freed under a handshake that still reads them.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeEndsStalledHandshakeFirst(CancellationToken cancellationToken)
    {
        using var meter = new Meter("Squirix");
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, LocalNodeId);
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var listening = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var clientHello = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var connectionClosed = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stub = RunStubPeerAsync(listening, clientHello, connectionClosed, cancellationToken);
        var port = await listening.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        var peerUri = new Uri($"https://127.0.0.1:{port}");
        var peer = new ServerPeer { NodeId = PeerNodeId, Uri = peerUri, InterNodeUri = peerUri };
        var args = new ServerClientPoolArgs
        {
            PolicyFactory = static _ => new IdleCallPolicy(),
            Certificate = material,
            InterNodeMtlsEnabled = true,
            MtlsOptions = new MtlsOptions { InternalListenPort = port },
        };
        var pool = new ServerClientPool([peer], args, new ServerClientPoolMetrics(meter), NullLogger<ServerClientPool>.Instance);
        var gateway = new ReplicaRpcGateway(pool);
        var call = gateway.AppendEntriesAsync(PeerNodeId, new ReplicaRpcHeader(LocalNodeId, ReadOnlyMemory<byte>.Empty, 1, 1, LocalNodeId, LocalNodeId), new FollowerBatch([], LocalNodeId, 1, 0, 0, 0), cancellationToken);
        await clientHello.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);

        await pool.DisposeAsync();

        _ = await Assert.That(pool.OpenConnections).IsEqualTo(0).Because("Disposal must return only once the stalled connection was torn down.");
        _ = await Assert.That(material.IsReleased).IsFalse().Because("The loader still holds the material after the pool released its hold.");
        DisposeAsLoader(material);
        _ = await Assert.That(material.IsReleased).IsTrue().Because("The pool must have released its hold once the stalled connection closed.");
        await connectionClosed.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<RpcException>(call);
        await stub;
    }

    /// <summary>Releases the loader's hold the way the DI container does on host shutdown.</summary>
    /// <param name="material">The material.</param>
    private static void DisposeAsLoader(MtlsCertificate material)
    {
        IDisposable loader = material;
        loader.Dispose();
    }

    /// <summary>Listens on a loopback port, accepts one connection, reads the start of the ClientHello, never answers, and reports when the client closed the connection.</summary>
    /// <param name="listening">Completed with the bound port.</param>
    /// <param name="clientHello">Completed once the first handshake bytes arrived.</param>
    /// <param name="connectionClosed">Completed once the client closed or reset the connection.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The listener bound no TCP port, or the client closed the connection before sending anything.</exception>
    private static async Task RunStubPeerAsync(TaskCompletionSource<int> listening, TaskCompletionSource clientHello, TaskCompletionSource connectionClosed, CancellationToken cancellationToken)
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        _ = listening.TrySetResult(listener.LocalEndpoint is IPEndPoint endpoint ? endpoint.Port : throw new InvalidOperationException("The stub peer did not bind a TCP port."));

        using var client = await listener.AcceptSocketAsync(cancellationToken);
        var buffer = new byte[1024];
        var read = await client.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
        if (read == 0)
            throw new InvalidOperationException("The client closed the connection before sending a ClientHello.");

        _ = clientHello.TrySetResult();
        try
        {
            do
                read = await client.ReceiveAsync(buffer, SocketFlags.None, cancellationToken);
            while (read > 0);
        }
        catch (SocketException)
        {
            // A reset is as good as an orderly close: the client is gone either way.
        }

        _ = connectionClosed.TrySetResult();
    }
}
