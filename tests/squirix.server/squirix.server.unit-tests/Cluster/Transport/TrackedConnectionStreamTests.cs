using System;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Connection tracking of the owned internode handlers: a connect that fails before a stream exists leaves no entry behind.</summary>
[Immutable]
public sealed class TrackedConnectionStreamTests : ServerUnitTestBase
{
    /// <summary>A refused connect leaves the connection count at zero.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedConnectLeavesNoConnection(CancellationToken cancellationToken)
    {
        var connections = new TrackedConnections();
        using var handler = CreateHandler(connections);
        using var client = new HttpClient(handler);

        _ = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetAsync(new Uri($"https://127.0.0.1:{ReserveClosedPort()}/"), cancellationToken));

        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>A connect after the connections were closed is refused with an object-disposed failure and is not counted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConnectAfterCloseIsRefused(CancellationToken cancellationToken)
    {
        var connections = new TrackedConnections();
        connections.Close();
        using var handler = CreateHandler(connections);
        using var client = new HttpClient(handler);

        var failure = await NodeAsyncAssert.ThrowsAsync<HttpRequestException>(client.GetAsync(new Uri($"https://127.0.0.1:{ReserveClosedPort()}/"), cancellationToken));

        _ = await Assert.That(failure.InnerException).IsTypeOf<ObjectDisposedException>();
        _ = await Assert.That(connections.Pending).IsEqualTo(0);
    }

    /// <summary>
    /// A stream whose base constructor rejected the socket is still finalized; the finalizer must neither crash the process nor leave a
    /// connection it never tracked.
    /// </summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Test]
    public async Task RejectedStreamFinalizerIsHarmless()
    {
        var connections = new TrackedConnections();
        connections.Enter();

        ConstructOverUnconnectedSocket(connections);
#pragma warning disable S1215 // The finalizer is the behavior under test; only a collection runs it.
        GC.Collect();
#pragma warning restore S1215
        GC.WaitForPendingFinalizers();

        _ = await Assert.That(connections.Pending).IsEqualTo(1);
        connections.Exit();
    }

    [MethodImpl(MethodImplOptions.NoInlining)]
    private static void ConstructOverUnconnectedSocket(TrackedConnections connections)
    {
        using var socket = new Socket(SocketType.Stream, ProtocolType.Tcp);
        _ = NodeExceptionAssert.For<IOException>().Throws((Socket: socket, Connections: connections), static state => _ = new TrackedConnectionStream(state.Socket, state.Connections));
    }

    private static SocketsHttpHandler CreateHandler(TrackedConnections connections) => new()
    {
        ConnectCallback = (context, cancellationToken) => TrackedConnectionStream.ConnectAsync(connections, context, cancellationToken),
    };

    private static int ReserveClosedPort()
    {
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener.LocalEndpoint is IPEndPoint endpoint ? endpoint.Port : throw new InvalidOperationException("The listener bound no TCP port.");
    }
}
