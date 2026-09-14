using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
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

    /// <summary>A stale term reroutes once to the next endpoint under the shared deadline.</summary>
    [Fact]
    public async Task StaleTermReroutesOnceWithOperationId()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-stale-1");

        var value = await failover.ExecuteWithDeadlineAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal)
                    ? throw new RpcException(new Status(StatusCode.FailedPrecondition, "stale-term"))
                    : new ValueTask<int>(42);
            },
            observer,
            TimeSpan.FromSeconds(30),
            null,
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, observer.Observed.Count);
        Assert.Equal(observer.OperationId, observer.Observed[0].OperationId);
        Assert.Equal(observer.OperationId, observer.Observed[1].OperationId);
    }

    /// <summary>A second stale term propagates instead of bouncing between endpoints.</summary>
    [Fact]
    public async Task SecondStaleTermPropagatesWithoutBouncing()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-stale-2");

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteWithDeadlineAsync<OperationObserver, int>(
                static (nodeId, state, _) =>
                {
                    state.Observed.Add((nodeId, state.OperationId));
                    throw new RpcException(new Status(StatusCode.FailedPrecondition, "stale-term"));
                },
                observer,
                TimeSpan.FromSeconds(30),
                null,
                DefaultCancellationToken));

        Assert.Equal(StatusCode.FailedPrecondition, error.StatusCode);
        Assert.Equal(2, observer.Observed.Count);
    }

    /// <summary>An expired shared deadline rejects the operation before the first attempt.</summary>
    [Fact]
    public async Task ExpiredDeadlineRejectsBeforeFirstAttempt()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-expired-1");

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteWithDeadlineAsync(
                static (nodeId, state, _) =>
                {
                    state.Observed.Add((nodeId, state.OperationId));
                    return new ValueTask<int>(42);
                },
                observer,
                TimeSpan.FromSeconds(-1),
                null,
                DefaultCancellationToken));

        Assert.Equal(StatusCode.DeadlineExceeded, error.StatusCode);
        Assert.Empty(observer.Observed);
    }

    /// <summary>An HTTP transport failure reroutes to the next endpoint.</summary>
    [Fact]
    public async Task HttpFailureFailsOverToNextEndpoint()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-http-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal)
                    ? throw new HttpRequestException("down")
                    : new ValueTask<int>(42);
            },
            observer,
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, observer.Observed.Count);
    }

    /// <summary>An I/O failure reroutes to the next endpoint.</summary>
    [Fact]
    public async Task IoFailureFailsOverToNextEndpoint()
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-io-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal)
                    ? throw new IOException("down")
                    : new ValueTask<int>(42);
            },
            observer,
            DefaultCancellationToken);

        Assert.Equal(42, value);
        Assert.Equal(2, observer.Observed.Count);
    }

    /// <summary>An unknown bootstrap primary is a configuration error.</summary>
    [Fact]
    public void UnknownPrimaryThrows()
    {
        _ = ExceptionAssert.For<InvalidOperationException>().Throws(
            BootstrapEndpoints,
            static endpoints => _ = new EndpointFailover(endpoints, "unknown"));
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
