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
        try
        {
            _ = await cluster.StartNodeAsync("nodeA", cancellationToken: cancellationToken);

            await cluster.StopNodeAsync("nodeA");
            await cluster.StopNodeAsync("nodeA");

            AssertPortReleased(held.Port);
        }
        finally
        {
            await cluster.DisposeAsync();
        }

        AssertPortReleased(held.Port);
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
