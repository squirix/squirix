using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Unit tests for bootstrap endpoint failover routing.</summary>
public sealed class EndpointFailoverTests : UnitTestBase
{
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>Verifies failover moves active traffic to the next bootstrap endpoint on transport errors.</summary>
    [Fact]
    public async Task ClientFailsOverAfterSelectedEndpointUnavailable()
    {
        var failover = new EndpointFailover(["endpoint-0", "endpoint-1"], "endpoint-0");
        var callCount = new MutableCallCount();

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Value++;
                var equals = string.Equals(nodeId, "endpoint-0", StringComparison.OrdinalIgnoreCase);
                return equals ? throw new RpcException(new Status(StatusCode.Unavailable, "down")) : new ValueTask<int>(42);
            },
            callCount,
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, callCount.Value);
    }

    /// <summary>Verifies non-transport errors do not trigger bootstrap failover.</summary>
    [Fact]
    public async Task DoesNotFailOverOnApplicationLevelRpcErrors()
    {
        var failover = new EndpointFailover(["endpoint-0", "endpoint-1"], "endpoint-0");

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteAsync<int>(static (_, _) => throw new RpcException(new Status(StatusCode.NotFound, "missing")), DefaultCancellationToken));

        Assert.Equal(StatusCode.NotFound, error.StatusCode);
    }

    private sealed class MutableCallCount
    {
        internal int Value { get; set; }
    }
}
