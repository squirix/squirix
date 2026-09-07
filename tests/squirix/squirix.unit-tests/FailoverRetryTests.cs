using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Bootstrap failover preserves the logical operation across endpoint switches.</summary>
[Immutable]
public sealed class FailoverRetryTests : UnitTestBase
{
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>Switching bootstrap endpoints reuses the same operation id for idempotent recovery.</summary>
    [Fact]
    public async Task BootstrapSwitchPreservesOperationId()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-preserve-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal)
                    ? throw new RpcException(new Status(StatusCode.Unavailable, "down"))
                    : new ValueTask<int>(42);
            },
            observer,
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, observer.Observed.Count);
        Assert.Equal("endpoint-0", observer.Observed[0].NodeId);
        Assert.Equal("endpoint-1", observer.Observed[1].NodeId);
        Assert.Equal(observer.OperationId, observer.Observed[0].OperationId);
        Assert.Equal(observer.OperationId, observer.Observed[1].OperationId);
    }

    private sealed class OperationObserver
    {
        internal OperationObserver(string operationId)
        {
            OperationId = operationId;
        }

        internal List<(string NodeId, string OperationId)> Observed { get; } = [];

        internal string OperationId { get; }
    }
}
