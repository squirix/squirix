using System;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Xunit;

namespace Squirix.IntegrationTests;

/// <summary>Client-only transport integration coverage for cluster peer pool warm-up.</summary>
public sealed class ClientPoolWarmUpTests : IntegrationTestBase
{
    private static readonly BootstrapConnectOptions FailFastConnectOptions = new(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200));

    /// <summary>Verifies warm-up fails when no bootstrap endpoint can be reached.</summary>
    [Fact]
    public async Task WarmUpFailsFastWhenPeerEndpointIsUnreachable()
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "peer-0",
                Uri = new Uri("https://127.0.0.1:1"),
            },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy(), connectOptions: FailFastConnectOptions);
        var exception = await AsyncAssert.ThrowsAsync<InvalidOperationException, string>(pool.WarmUpAsync(DefaultCancellationToken));
        Assert.Contains("Failed to connect to endpoint", exception.Message, StringComparison.Ordinal);
    }
}
