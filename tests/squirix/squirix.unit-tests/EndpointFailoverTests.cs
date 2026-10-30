using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Unit tests for bootstrap endpoint failover routing.</summary>
[Immutable]
public sealed class EndpointFailoverTests
{
    private static readonly string[] BootstrapEndpoints = ["endpoint-0", "endpoint-1"];

    /// <summary>Verifies failover moves active traffic to the next bootstrap endpoint on transport errors.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailsOverWhenSelectedEndpointDown(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), TimeProvider.System);
        var callCount = new MutableCallCount();

        var value = await failover.ExecuteAsync(
            static (nodeId, state, _, _) =>
            {
                state.Value++;
                var equals = string.Equals(nodeId, "endpoint-0", StringComparison.OrdinalIgnoreCase);
                return equals ? throw new RpcException(new Status(StatusCode.Unavailable, "down")) : new ValueTask<int>(42);
            },
            callCount,
            cancellationToken);

        _ = await Assert.That(value).IsEqualTo(42);
        _ = await Assert.That(callCount.Value).IsEqualTo(2);

        var nextEndpoint = await failover.ExecuteAsync(static (nodeId, _, _, _) => ValueTask.FromResult(nodeId), 0, cancellationToken);
        _ = await Assert.That(nextEndpoint).IsEqualTo("endpoint-1");
    }

    /// <summary>Concurrent reroutes publish a valid endpoint that subsequent calls reuse.</summary>
    [Test]
    public async Task ConcurrentReroutesKeepActiveEndpoint()
    {
        const int callers = 16;
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), TimeProvider.System);
        using var arrived = new SemaphoreSlim(0);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = new Task<string>[callers];
        for (var i = 0; i < callers; i++)
        {
            calls[i] = failover.ExecuteAsync(
                static async (nodeId, state, budget, ct) =>
                {
                    _ = budget;
                    if (string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal))
                        throw new RpcException(new Status(StatusCode.Unavailable, "down"));

                    _ = state.Ready.Release();
                    await state.Release.WaitAsync(ct).ConfigureAwait(false);
                    return nodeId;
                },
                (Ready: arrived, Release: release.Task),
                CancellationToken.None).AsTask();
        }

        try
        {
            for (var i = 0; i < callers; i++)
                _ = await Assert.That(await arrived.WaitAsync(TimeSpan.FromSeconds(5), CancellationToken.None)).IsTrue();
        }
        finally
        {
            _ = release.TrySetResult();
        }

        var endpoints = await Task.WhenAll(calls).WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);
        foreach (var endpoint in endpoints)
            _ = await Assert.That(endpoint).IsEqualTo("endpoint-1");
        var next = await failover.ExecuteAsync(static (nodeId, _, _, _) => ValueTask.FromResult(nodeId), 0, CancellationToken.None);
        _ = await Assert.That(next).IsEqualTo("endpoint-1");
    }

    /// <summary>Switching endpoints keeps the time spent on the failed endpoint in the operation budget.</summary>
    [Test]
    public async Task FailoverKeepsElapsedBudget()
    {
        var clock = new FakeTimeProvider(new DateTimeOffset(2000, 1, 1, 0, 0, 0, TimeSpan.Zero));
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), clock);

        var remaining = await failover.ExecuteAsync(
            static (nodeId, state, budget, _) =>
            {
                if (string.Equals(nodeId, "endpoint-0", StringComparison.Ordinal))
                {
                    state.Advance(TimeSpan.FromSeconds(10));
                    throw new RpcException(new Status(StatusCode.Unavailable, "down"));
                }

                return ValueTask.FromResult(budget.Remaining);
            },
            clock,
            CancellationToken.None);

        _ = await Assert.That(remaining).IsEqualTo(TimeSpan.FromSeconds(20));
    }

    /// <summary>Verifies non-transport errors do not trigger bootstrap failover.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NoFailOverOnApplicationRpcErrors(CancellationToken cancellationToken)
    {
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), TimeProvider.System);

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteAsync<int, int>(static (_, _, _, _) => throw new RpcException(new Status(StatusCode.NotFound, "missing")), 0, cancellationToken));

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
        var failover = new EndpointFailover(BootstrapEndpoints, "endpoint-0", TimeSpan.FromSeconds(30), TimeProvider.System);
        var callCount = new MutableCallCount();

        var error = await AsyncAssert.ThrowsAsync<RpcException, int>(
            failover.ExecuteAsync<MutableCallCount, int>(
                static (_, state, _, _) =>
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

    /// <summary>Verifies the operation deadline must be finite and positive.</summary>
    [Test]
    public void ConstructorRejectsNonPositiveDeadline()
    {
        _ = ExceptionAssert.For<ArgumentOutOfRangeException>().Throws(BootstrapEndpoints, static endpoints => _ = new EndpointFailover(endpoints, "endpoint-0", TimeSpan.Zero, TimeProvider.System));
        _ = ExceptionAssert.For<ArgumentOutOfRangeException>().Throws(BootstrapEndpoints, static endpoints => _ = new EndpointFailover(endpoints, "endpoint-0", TimeSpan.FromSeconds(-1), TimeProvider.System));
        _ = ExceptionAssert.For<ArgumentOutOfRangeException>().Throws(BootstrapEndpoints, static endpoints => _ = new EndpointFailover(endpoints, "endpoint-0", Timeout.InfiniteTimeSpan, TimeProvider.System));
    }

    private sealed class MutableCallCount
    {
        internal int Value { get; set; }
    }
}
