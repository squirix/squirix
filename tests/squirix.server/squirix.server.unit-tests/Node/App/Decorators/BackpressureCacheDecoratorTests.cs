using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Rocks;
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
            new BackpressureMetrics(_testMeter));

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Get, "jwt:client-a", cancellationToken)).Lease;

        var getValueCalls = 0;
        var innerExpectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = innerExpectations.Setups.GetValueAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<CancellationToken>())
                             .Callback((_, _, _) =>
                              {
                                  _ = Interlocked.Increment(ref getValueCalls);
                                  return ValueTask.FromResult(new NodeCacheValueResult<string>(false, null));
                              });
        var inner = innerExpectations.Instance();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException, NodeCacheValueResult<string>>(cacheA.GetValueAsync("c", "k", cancellationToken));
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
            new BackpressureMetrics(_testMeter));

        using var held = (await gate.AcquireAsync("cache", CacheOperationNames.Set, "jwt:client-a", cancellationToken)).Lease;

        var setEntryCalls = 0;
        var innerExpectations = new ILogicalNamespacedCacheCreateExpectations<string>();
        _ = innerExpectations.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<string>>(), Arg.Any<CancellationToken>())
                             .Callback((_, _, _, _, _) =>
                              {
                                  _ = Interlocked.Increment(ref setEntryCalls);
                                  return ValueTask.CompletedTask;
                              });
        var inner = innerExpectations.Instance();
        var cacheA = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-a"));
        var cacheB = new BackpressureCacheDecorator<string>(inner, gate, CreateClientIdResolver("jwt:client-b"));
        var entry = new NodeCacheEntry<string>("value");

        var rejected = await NodeAsyncAssert.ThrowsAsync<SquirixException>(cacheA.SetEntryAsync("op-1", "c", "k", entry, cancellationToken));
        _ = await Assert.That(rejected.Code).IsEqualTo(SquirixErrorCode.TooManyRequests);
        _ = await Assert.That(Volatile.Read(ref setEntryCalls)).IsEqualTo(0);

        await cacheB.SetEntryAsync("op-2", "c", "k", entry, cancellationToken);
        _ = await Assert.That(Volatile.Read(ref setEntryCalls)).IsEqualTo(1);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static IBackpressureClientIdResolver CreateClientIdResolver(string clientId)
    {
        var expectations = new IBackpressureClientIdResolverCreateExpectations();
        _ = expectations.Setups.Resolve().ReturnValue(clientId);
        return expectations.Instance();
    }
}
