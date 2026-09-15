using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
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

/// <summary>Bootstrap failover preserves the logical operation across endpoint switches.</summary>
[Immutable]
public sealed class FailoverRetryTests : UnitTestBase
{
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>Switching bootstrap endpoints reuses the same operation id for idempotent recovery.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BootstrapSwitchPreservesOperationId(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-preserve-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal) ? throw new RpcException(new Status(StatusCode.Unavailable, "down")) : new ValueTask<int>(42);
            },
            observer,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(observer.Observed.Count).IsEqualTo(2);
        _ = await Assert.That(observer.Observed[0].NodeId).IsEqualTo("endpoint-0");
        _ = await Assert.That(observer.Observed[1].NodeId).IsEqualTo("endpoint-1");
        _ = await Assert.That(observer.Observed[0].OperationId).IsEqualTo(observer.OperationId);
        _ = await Assert.That(observer.Observed[1].OperationId).IsEqualTo(observer.OperationId);
    }

    /// <summary>An expired shared deadline rejects the operation before the first attempt.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredDeadlineRejectsBeforeFirstAttempt(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.DeadlineExceeded);
        _ = await Assert.That(observer.Observed).IsEmpty();
    }

    /// <summary>An HTTP transport failure reroutes to the next endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HttpFailureFailsOverToNextEndpoint(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-http-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal) ? throw new HttpRequestException("down") : new ValueTask<int>(42);
            },
            observer,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(observer.Observed.Count).IsEqualTo(2);
    }

    /// <summary>An I/O failure reroutes to the next endpoint.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IoFailureFailsOverToNextEndpoint(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-io-1");

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal) ? throw new IOException("down") : new ValueTask<int>(42);
            },
            observer,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(observer.Observed.Count).IsEqualTo(2);
    }

    /// <summary>A second stale term propagates instead of bouncing between endpoints.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondStaleTermPropagatesWithoutBouncing(CancellationToken cancellationToken)
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
                cancellationToken));

        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(observer.Observed.Count).IsEqualTo(2);
    }

    /// <summary>A stale term reroutes once to the next endpoint under the shared deadline.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleTermReroutesOnceWithOperationId(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0");
        var observer = new OperationObserver("operation-stale-1");

        var value = await failover.ExecuteWithDeadlineAsync(
            static (nodeId, state, _) =>
            {
                state.Observed.Add((nodeId, state.OperationId));
                return string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal) ? throw new RpcException(new Status(StatusCode.FailedPrecondition, "stale-term"))
                    : new ValueTask<int>(42);
            },
            observer,
            TimeSpan.FromSeconds(30),
            null,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(observer.Observed.Count).IsEqualTo(2);
        _ = await Assert.That(observer.Observed[0].OperationId).IsEqualTo(observer.OperationId);
        _ = await Assert.That(observer.Observed[1].OperationId).IsEqualTo(observer.OperationId);
    }

    /// <summary>An unknown bootstrap primary is a configuration error.</summary>
    [Test]
    public void UnknownPrimaryThrows() =>
        _ = ExceptionAssert.For<InvalidOperationException>().Throws(BootstrapEndpoints, static endpoints => _ = new EndpointFailover(endpoints, "unknown"));

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
