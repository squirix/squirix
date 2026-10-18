using System;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Failover away from a bootstrap endpoint whose host accepts connections but never answers.</summary>
[Immutable]
public sealed class EndpointFailoverDialTests
{
    private static readonly TimeSpan AcceptBound = TimeSpan.FromSeconds(30);
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>
    /// The client's own handler and the production retry budget of a client with two endpoints fail a call whose first endpoint never answers
    /// the TLS handshake over to the next endpoint after one connect bound (two seconds), instead of connecting to the first one again. The
    /// bound of four seconds leaves slack for a loaded host and stays below two connect bounds; retrying the first endpoint would take three.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SilentEndpointFailsOverQuickly(CancellationToken cancellationToken)
    {
        // Neither listener ever accepts: the kernel completes the TCP handshake, and the TLS handshake that follows gets no answer.
        using var silent = Listen();
        using var next = Listen();
        var peers = new[]
        {
            new Peer { NodeId = "endpoint-0", Uri = UriOf(silent) },
            new Peer { NodeId = "endpoint-1", Uri = UriOf(next) },
        };
        var pool = new ClientPool(peers, static peer => CallPolicyDefaults.Create(peer, TimeProvider.System), TimeProvider.System);
        await using var owner = pool;
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), TimeProvider.System);
        using var call = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var stopwatch = Stopwatch.StartNew();

        var operation = failover.ExecuteAsync(
            static (nodeId, hasMoreEndpoints, state, ct) => state.PolicyFor(nodeId).ExecuteAsync(
                static async (client, token) =>
                {
                    _ = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "cache", Key = "key" }, cancellationToken: token);
                    return 0;
                },
                state.ForNode(nodeId),
                hasMoreEndpoints,
                ct),
            pool,
            call.Token);
        using var reached = await next.AcceptSocketAsync(cancellationToken).AsTask().WaitAsync(AcceptBound, TimeProvider.System, cancellationToken);
        var elapsed = stopwatch.Elapsed;
        await call.CancelAsync();
        _ = await AsyncAssert.ThrowsAnyAsync<Exception, int>(operation);

        _ = await Assert.That(elapsed).IsLessThan(TimeSpan.FromSeconds(4));
    }

    private static TcpListener Listen()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        return listener;
    }

    private static Uri UriOf(TcpListener listener) =>
        new UriBuilder(Uri.UriSchemeHttps, IPAddress.Loopback.ToString(), listener.LocalEndpoint is IPEndPoint endpoint ? endpoint.Port : 0).Uri;
}
