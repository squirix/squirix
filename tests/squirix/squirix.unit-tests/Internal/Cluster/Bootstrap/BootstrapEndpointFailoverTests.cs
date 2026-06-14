using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal.Cluster.Bootstrap;
using Xunit;

namespace Squirix.UnitTests.Internal.Cluster.Bootstrap;

/// <summary>
/// Unit tests for bootstrap endpoint failover routing.
/// </summary>
public sealed class BootstrapEndpointFailoverTests : UnitTestBase
{
    /// <summary>
    /// Verifies failover moves active traffic to the next bootstrap endpoint on transport errors.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task ClientFailsOverAfterSelectedEndpointUnavailable()
    {
        var failover = new BootstrapEndpointFailover(["endpoint-0", "endpoint-1"], "endpoint-0");
        var calls = 0;

        var value = await failover.ExecuteAsync(
            (nodeId, _) =>
            {
                calls++;
                return string.Equals(nodeId, "endpoint-0", StringComparison.OrdinalIgnoreCase) ? throw new RpcException(new Status(StatusCode.Unavailable, "down"))
                    : new ValueTask<int>(42);
            },
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, calls);
    }

    /// <summary>
    /// Verifies non-transport errors do not trigger bootstrap failover.
    /// </summary>
    /// <returns>A task that completes when assertions pass.</returns>
    [Fact]
    public async Task DoesNotFailOverOnApplicationLevelRpcErrors()
    {
        var failover = new BootstrapEndpointFailover(["endpoint-0", "endpoint-1"], "endpoint-0");

        var error = await Assert.ThrowsAsync<RpcException>(() =>
            failover.ExecuteAsync<int>(static (_, _) => throw new RpcException(new Status(StatusCode.NotFound, "missing")), DefaultCancellationToken).AsTask());

        Assert.Equal(StatusCode.NotFound, error.StatusCode);
    }
}
