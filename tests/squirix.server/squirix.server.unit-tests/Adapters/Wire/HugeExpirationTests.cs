using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Adapters.Wire;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using TUnit.Mocks;
using TUnit.Mocks.Arguments;
using TUnit.Mocks.Generated;

namespace Squirix.Server.UnitTests.Adapters.Wire;

/// <summary>A relative expiration up to TimeSpan.MaxValue reaches the cache unchanged.</summary>
public sealed class HugeExpirationTests : DisposableServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Self = "node-a";
    private const string CacheName = "c";
    private const string Key = "k";
    private const long FourMillionDaysTicks = 4_000_000L * TimeSpan.TicksPerDay;

    private readonly Meter _testMeter = new("test-huge-expiration");

    /// <summary>Touch with an extended expiration reaches the cache with the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    [Test]
    [Arguments(FourMillionDaysTicks)]
    [Arguments(long.MaxValue)]
    public async Task TouchPreservesExpiration(long ticks)
    {
        TimeSpan? seen = null;
        var api = ICacheApi<StoredValue>.Mock(MockBehavior.Strict);
        _ = api.TouchAsync(string.Empty, Key, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .ReturnsAsync((_, _, expiration, _) =>
            {
                seen = expiration;
                return ValueTask.FromResult(true);
            });

        var response = await CreateAdapter(api.Object).Mutations.TouchAsync(
            new TouchAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Expiration = TimeSpan.FromTicks(ticks) },
            CancellationToken.None);

        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(seen).IsEqualTo(TimeSpan.FromTicks(ticks));
    }

    /// <summary>SetEntry with an extended expiration reaches the cache with the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    [Test]
    [Arguments(FourMillionDaysTicks)]
    [Arguments(long.MaxValue)]
    public async Task SetEntryPreservesExpiration(long ticks)
    {
        TimeSpan? seen = null;
        var api = ICacheApi<StoredValue>.Mock(MockBehavior.Strict);
        _ = api.SetEntryAsync(string.Empty, Key, Arg.Any<NodeCacheEntry<StoredValue>>(), Arg.Any<CancellationToken>())
            .ReturnsAsync((_, _, entry, _) =>
            {
                seen = entry.Expiration;
                return ValueTask.CompletedTask;
            });

        _ = await CreateAdapter(api.Object).Mutations.SetEntryAsync(
            new SetEntryAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Entry = CreateEntry(TimeSpan.FromTicks(ticks)) },
            CancellationToken.None);

        _ = await Assert.That(seen).IsEqualTo(TimeSpan.FromTicks(ticks));
    }

    /// <summary>TryAddEntry with an extended expiration reaches the cache with the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    [Test]
    [Arguments(FourMillionDaysTicks)]
    [Arguments(long.MaxValue)]
    public async Task AddEntryPreservesExpiration(long ticks)
    {
        TimeSpan? seen = null;
        var api = ICacheApi<StoredValue>.Mock(MockBehavior.Strict);
        _ = api.TryAddEntryAsync(string.Empty, Key, Arg.Any<NodeCacheEntry<StoredValue>>(), Arg.Any<CancellationToken>())
            .ReturnsAsync((_, _, entry, _) =>
            {
                seen = entry.Expiration;
                return ValueTask.FromResult(true);
            });

        var response = await CreateAdapter(api.Object).Mutations.AddEntryIfAbsentAsync(
            new TryAddEntryAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Entry = CreateEntry(TimeSpan.FromTicks(ticks)) },
            CancellationToken.None);

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(seen).IsEqualTo(TimeSpan.FromTicks(ticks));
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static CacheEntryWire CreateEntry(TimeSpan expiration) => new() { Value = JsonValueKit.Wire("{\"__v\":\"v\"}"), Expiration = expiration };

    private static RoutedCacheOperations<StoredValue> CreateAdapter(ICacheApi<StoredValue> api)
    {
        var operations = IInboundEndpointCacheOperations<StoredValue>.Mock(MockBehavior.Strict);
        _ = operations.ForCache(CacheName).Returns(api);
        var ownership = INodeOwnershipResolver.Mock(MockBehavior.Strict);
        _ = ownership.SelfNodeId.Returns(Self);
        _ = ownership.GetOwner(Arg.Any<string>(), Arg.Any<string>()).Returns(Self);
        var invocation = IRemoteInvocationState.Mock(MockBehavior.Strict);
        _ = invocation.IsInternalOwnerInvocation.Returns(false);

        return new RoutedCacheOperations<StoredValue>(
            operations.Object,
            OwnerRouters.Static(ownership.Object, invocation.Object, Self),
            new OwnerRpcForwarder(
                IServerClientPool.Mock(MockBehavior.Strict).Object,
                IBackpressureGate.Mock(MockBehavior.Strict).Object,
                IBackpressureClientIdResolver.Mock(MockBehavior.Strict).Object,
                RingAgreements.Create()),
            false,
            TimeProvider.System);
    }
}
