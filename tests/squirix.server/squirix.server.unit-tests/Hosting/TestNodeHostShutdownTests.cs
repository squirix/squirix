using System.Net;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies graceful node shutdown and its interplay with disposal.</summary>
public sealed class TestNodeHostShutdownTests
{
    /// <summary>Shutdown stops the node and stays safe to repeat and to follow with disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ShutdownAsyncStopsNodeIdempotently(CancellationToken cancellationToken)
    {
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        var cluster = TestCluster<ClusterStartOptions>.Create(new ClusterNode("nodeA", held.HttpUri));
        var host = await cluster.StartNodeAsync("nodeA", cancellationToken: cancellationToken);
        try
        {
            // Two direct ShutdownAsync calls on the same host must both be no-throw and idempotent.
            await host.ShutdownAsync();
            await host.ShutdownAsync();

            AssertPortReleased(held.Port);

            // Disposing an already-shut-down host must also be a safe no-op.
            await host.DisposeAsync();

            AssertPortReleased(held.Port);
        }
        finally
        {
            // The host is still registered on the cluster (StopNodeAsync was never called), so this
            // exercises a third idempotent shutdown through the cluster's own teardown path.
            await cluster.DisposeAsync();
        }
    }

    private static void AssertPortReleased(int port)
    {
        using var listener = new TcpListener(IPAddress.Loopback, port);
        listener.Server.ExclusiveAddressUse = true;
        listener.Server.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, false);
        listener.Start();
        listener.Stop();
    }
}
