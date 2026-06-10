using System;
using System.Threading.Tasks;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Xunit;

namespace Squirix.IntegrationTests.Transport;

/// <summary>
/// Client-only transport integration coverage for cluster peer pool warm-up.
/// </summary>
public sealed class ClientPoolWarmUpTests : IntegrationTestBase
{
    private static readonly BootstrapConnectOptions FailFastConnectOptions = new(TimeSpan.FromMilliseconds(50), TimeSpan.FromMilliseconds(200));

    /// <summary>
    /// Verifies warm-up fails when no bootstrap endpoint can be reached.
    /// </summary>
    /// <returns>A task that completes when the test finishes.</returns>
    [Fact]
    public async Task WarmUpFailsFastWhenPeerEndpointIsUnreachable()
    {
        var peers = new[]
        {
            new Peer
            {
                NodeId = "peer-0",
                Url = "https://127.0.0.1:1",
            },
        };

        await using var pool = new ClientPool(peers, static _ => new CallPolicy(), connectOptions: FailFastConnectOptions);
        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => pool.WarmUpAsync(DefaultCancellationToken).AsTask());
        Assert.Contains("Failed to connect to endpoint", exception.Message, StringComparison.Ordinal);
    }
}
