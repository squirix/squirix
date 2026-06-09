using System;
using System.Net.Http;
using System.Threading.Tasks;
using Squirix.E2ETests.Infrastructure;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Server.TestKit.Http;
using Xunit;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Fast bootstrap warm-up coverage using internal <see cref="ClientPool" /> APIs and fail-fast connect options.
/// </summary>
public sealed class ClientPoolBootstrapWarmUpTests : E2ETestBase
{
    private static readonly BootstrapConnectOptions FailFastConnectOptions = new(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200));
    private static readonly SocketsHttpHandler LoopbackHandler = LoopbackHttp.CreateHandler();

    /// <summary>
    /// Verifies warm-up succeeds when any configured bootstrap endpoint is reachable and skips dead peers quickly.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task WarmUpSucceedsWhenAnyBootstrapEndpointIsReachable()
    {
        await using var cluster = await E2ECluster.StartSingleNodeAsync(nameof(WarmUpSucceedsWhenAnyBootstrapEndpointIsReachable), cancellationToken: DefaultCancellationToken);
        var liveUrl = cluster.GetAddress("nodeA");

        var peers = new[]
        {
            new Peer { NodeId = "nodeA", Url = liveUrl },
            new Peer { NodeId = "peer-dead", Url = "https://127.0.0.1:1" },
        };

        await using var pool = new ClientPool(peers, static id => new CallPolicy(peer: id), LoopbackHandler, connectOptions: FailFastConnectOptions);
        var primaryNodeId = await pool.WarmUpAsync(DefaultCancellationToken);
        Assert.Equal("nodeA", primaryNodeId);
    }
}
