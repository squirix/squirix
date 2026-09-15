using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for bootstrap endpoint failover routing.</summary>
[Immutable]
public sealed class EndpointFailoverTests : UnitTestBase
{
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>Verifies failover moves active traffic to the next bootstrap endpoint on transport errors.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailsOverWhenSelectedEndpointDown(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var callCount = new MutableCallCount();

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Value++;
                var equals = string.Equals(nodeId, "endpoint-0", StringComparison.OrdinalIgnoreCase);
                return equals ? throw new RpcException(new Status(StatusCode.Unavailable, "down")) : new ValueTask<int>(42);
            },
            callCount,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(callCount.Value).IsEqualTo(2);
    }

    /// <summary>Verifies non-transport errors do not trigger bootstrap failover.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFailOverOnApplicationRpcErrors(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteAsync<int>(static (_, _) => throw new RpcException(new Status(StatusCode.NotFound, "missing")), cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.NotFound);
    }

    /// <summary>
    /// Verifies an ambiguous commit outcome never fails over: the next endpoint holds no idempotency
    /// record that would gate a re-execution of the already-possibly-committed mutation.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFailOverOnCommitOutcomeUnknown(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var callCount = new MutableCallCount();

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteAsync<MutableCallCount, int>(
                static (_, state, _) =>
                {
                    state.Value++;
                    throw new RpcException(new Status(StatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));
                },
                callCount,
                cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(error.Status.Detail).IsEqualTo(CommitOutcomeUnknownException.StableDetail);
        _ = await Assert.That(callCount.Value).IsEqualTo(1);
    }

    private sealed class MutableCallCount
    {
        internal int Value { get; set; }
    }
}
