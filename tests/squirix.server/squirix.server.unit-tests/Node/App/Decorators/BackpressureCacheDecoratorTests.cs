using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Node.App.Decorators;

/// <summary>Covers per-client isolation through <see cref="BackpressureCacheDecorator{T}" />.</summary>
[Immutable]
public sealed class BackpressureCacheDecoratorTests : DisposableServerUnitTestBase
{
    private readonly Meter _testMeter = new("test");

    /// <summary>Two client ids keep independent PerClientMaxInFlight budgets.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TwoClientIdsGetIndependentLimits(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(50),
            },
            new BackpressureMetrics(_testMeter),
            TimeProvider.System);

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Get, "jwt:client-a", cancellationToken)).Lease;

        var getValueCalls = 0;
        var innerExpectations = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
        _ = innerExpectations.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                             .ReturnsAsync((_, _, _) =>
                              {
                                  _ = Interlocked.Increment(ref getValueCalls);
                                  return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
                              });
        var inner = innerExpectations.Object;
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException, NodeCacheValueResult<string>>(cacheA.GetValueAsync("c", "k", cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejected.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);

        var otherClient = await cacheB.GetValueAsync("c", "k", cancellationToken);
        _ = await Assert.That(otherClient.Found).IsFalse();
        _ = await Assert.That(Volatile.Read(ref getValueCalls)).IsEqualTo(1);
    }

    /// <summary>The void write path enforces the same per-client budgets independently across client ids.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritesApplyIndependentPerClientCaps(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 4,
                PerClientMaxInFlight = 1,
                PerClientMaxQueue = 0,
                MaxQueue = 4,
                SlowdownThreshold = 4,
                MaxSlowdownDelay = TimeSpan.Zero,
                MaxQueueWait = TimeSpan.FromMilliseconds(50),
            },
            new BackpressureMetrics(_testMeter),
            TimeProvider.System);

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Set, "jwt:client-a", cancellationToken)).Lease;

        var setEntryCalls = 0;
        var innerExpectations = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
        _ = innerExpectations.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                             .ReturnsAsync((_, _, _, _, _) =>
                              {
                                  _ = Interlocked.Increment(ref setEntryCalls);
                                  return ValueTask.CompletedTask;
                              });
        var inner = innerExpectations.Object;
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));
        var entry = new NodeCacheEntry<string>("value");

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cacheA.SetEntryAsync("op-1", "c", "k", entry, cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejected.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(Volatile.Read(ref setEntryCalls)).IsEqualTo(0);

        await cacheB.SetEntryAsync("op-2", "c", "k", entry, cancellationToken);
        _ = await Assert.That(Volatile.Read(ref setEntryCalls)).IsEqualTo(1);
    }

    /// <summary>Nested reads and writes reuse the inbound slot before resolving the client id.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdmittedCallsSkipClientResolution(CancellationToken cancellationToken)
    {
        using var gate = new AdmissionGate(
            new AdmissionOptions
            {
                MaxInFlight = 1,
                MaxQueue = 0,
                SlowdownThreshold = 1,
                MaxSlowdownDelay = TimeSpan.Zero,
            },
            new BackpressureMetrics(_testMeter),
            TimeProvider.System);

        var (decision, lease) = await gate.AcquireAsync("cache", CacheOperationNames.Set, "jwt:client-a", cancellationToken);
        using (lease)
        {
            _ = await Assert.That(decision.IsAccepted).IsTrue();
            var entry = new NodeCacheEntry<string>("value");
            var inner = ILogicalNamespacedCache<string>.Mock(MockBehavior.Strict);
            _ = inner.GetValueAsync("c", "k", cancellationToken).ReturnsAsync(ValueTask.FromResult(new NodeCacheValueResult<string>(true, "value")));
            _ = inner.SetEntryAsync("op-1", "c", "k", entry, cancellationToken).ReturnsAsync(ValueTask.CompletedTask);
            var clients = IBackpressureClientIdResolver.Mock(MockBehavior.Strict);
            var cache = new BackpressureCacheDecorator<string>(inner.Object, gate, clients.Object);

            using (InboundAdmissionScope.Enter(gate))
            {
                await cache.SetEntryAsync("op-1", "c", "k", entry, cancellationToken);
                var read = await cache.GetValueAsync("c", "k", cancellationToken);
                _ = await Assert.That(read.Value).IsEqualTo("value");
                _ = await Assert.That(gate.InFlight).IsEqualTo(1);
                _ = await Assert.That(gate.QueueDepth).IsEqualTo(0);
            }

            inner.GetValueAsync("c", "k", cancellationToken).WasCalled(Times.Once);
            inner.SetEntryAsync("op-1", "c", "k", entry, cancellationToken).WasCalled(Times.Once);
        }

        _ = await Assert.That(gate.InFlight).IsEqualTo(0);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static IBackpressureClientIdResolver CreateClientIdResolver(string clientId)
    {
        var expectations = IBackpressureClientIdResolver.Mock(MockBehavior.Strict);
        _ = expectations.Resolve().Returns(clientId);
        return expectations.Object;
    }
}
