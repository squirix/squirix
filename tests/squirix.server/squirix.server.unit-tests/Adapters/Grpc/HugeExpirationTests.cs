using System;
using System.Diagnostics.Metrics;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Adapters.Grpc;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Runtime.Invocation;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Grpc;

/// <summary>A relative expiration beyond the normalized duration range saturates instead of failing the request.</summary>
public sealed class HugeExpirationTests : DisposableServerUnitTestBase
{
    private const string OperationId = "0123456789abcdef0123456789abcdef";
    private const string Self = "node-a";
    private const string CacheName = "c";
    private const string Key = "k";
    private const long MaxTicksSeconds = long.MaxValue / TimeSpan.TicksPerSecond;
    private const long FourMillionDaysSeconds = 4_000_000L * 86_400L;

    private readonly Meter _testMeter = new("test-huge-expiration");

    /// <summary>The largest normalized duration converts like the protobuf conversion.</summary>
    [Test]
    public async Task NormalizedLimitConvertsExactly()
    {
        var limit = new Duration { Seconds = WireDuration.MaxSeconds };

        _ = await Assert.That(WireDuration.ToTimeSpan(limit)).IsEqualTo(limit.ToTimeSpan());
    }

    /// <summary>A duration beyond the limit saturates to the maximum time span.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(MaxTicksSeconds, 0)]
    [Arguments(FourMillionDaysSeconds, 0)]
    [Arguments(long.MaxValue, 999_999_999)]
    public async Task HugeDurationSaturates(long seconds, int nanos)
    {
        var converted = WireDuration.ToTimeSpan(new Duration { Seconds = seconds, Nanos = nanos });

        _ = await Assert.That(converted).IsEqualTo(TimeSpan.MaxValue);
    }

    /// <summary>A time span beyond the limit is sent as the largest normalized duration.</summary>
    [Test]
    public async Task HugeTimeSpanClampsToLimit()
    {
        _ = await Assert.That(WireDuration.FromTimeSpan(TimeSpan.MaxValue).Seconds).IsEqualTo(WireDuration.MaxSeconds);
        _ = await Assert.That(WireDuration.FromTimeSpan(TimeSpan.FromDays(4_000_000)).Seconds).IsEqualTo(WireDuration.MaxSeconds);
        _ = await Assert.That(WireDuration.FromTimeSpan(TimeSpan.FromMinutes(1))).IsEqualTo(Duration.FromTimeSpan(TimeSpan.FromMinutes(1)));
    }

    /// <summary>Touch with an out-of-range expiration reaches the cache with a saturated expiration.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(MaxTicksSeconds, 0)]
    [Arguments(FourMillionDaysSeconds, 0)]
    [Arguments(long.MaxValue, 999_999_999)]
    public async Task TouchSaturates(long seconds, int nanos)
    {
        TimeSpan? seen = null;
        var api = new ICacheApiCreateExpectations<object?>();
        _ = api.Setups.TouchAsync(OperationId, Key, Arg.Any<TimeSpan>(), Arg.Any<CancellationToken>())
            .Callback((_, _, expiration, _) =>
            {
                seen = expiration;
                return ValueTask.FromResult(true);
            });

        var response = await CreateAdapter(api.Instance()).Touch(
            new TouchAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Expiration = new Duration { Seconds = seconds, Nanos = nanos } },
            new TestServerCallContext());

        _ = await Assert.That(response.Found).IsTrue();
        _ = await Assert.That(seen).IsEqualTo(TimeSpan.MaxValue);
    }

    /// <summary>SetEntry with an out-of-range expiration reaches the cache with a saturated expiration.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(MaxTicksSeconds, 0)]
    [Arguments(FourMillionDaysSeconds, 0)]
    [Arguments(long.MaxValue, 999_999_999)]
    public async Task SetEntrySaturates(long seconds, int nanos)
    {
        TimeSpan? seen = null;
        var api = new ICacheApiCreateExpectations<object?>();
        _ = api.Setups.SetEntryAsync(OperationId, Key, Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
            .Callback((_, _, entry, _) =>
            {
                seen = entry.Expiration;
                return ValueTask.CompletedTask;
            });

        _ = await CreateAdapter(api.Instance()).SetEntry(
            new SetEntryAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Entry = CreateEntry(new Duration { Seconds = seconds, Nanos = nanos }) },
            new TestServerCallContext());

        _ = await Assert.That(seen).IsEqualTo(TimeSpan.MaxValue);
    }

    /// <summary>TryAddEntry with an out-of-range expiration reaches the cache with a saturated expiration.</summary>
    /// <param name="seconds">The wire seconds.</param>
    /// <param name="nanos">The wire nanoseconds.</param>
    [Test]
    [Arguments(MaxTicksSeconds, 0)]
    [Arguments(FourMillionDaysSeconds, 0)]
    [Arguments(long.MaxValue, 999_999_999)]
    public async Task AddEntrySaturates(long seconds, int nanos)
    {
        TimeSpan? seen = null;
        var api = new ICacheApiCreateExpectations<object?>();
        _ = api.Setups.TryAddEntryAsync(OperationId, Key, Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
            .Callback((_, _, entry, _) =>
            {
                seen = entry.Expiration;
                return ValueTask.FromResult(true);
            });

        var response = await CreateAdapter(api.Instance()).TryAddEntry(
            new TryAddEntryAsyncRequest { CacheName = CacheName, Key = Key, OperationId = OperationId, Entry = CreateEntry(new Duration { Seconds = seconds, Nanos = nanos }) },
            new TestServerCallContext());

        _ = await Assert.That(response.Added).IsTrue();
        _ = await Assert.That(seen).IsEqualTo(TimeSpan.MaxValue);
    }

    /// <inheritdoc />
    protected override void DisposeManaged() => _testMeter.Dispose();

    private static CacheEntryWire CreateEntry(Duration expiration) => new() { Value = Struct.Parser.ParseJson("{\"__v\":\"v\"}"), Expiration = expiration };

    private SquirixServiceAdapter<object?> CreateAdapter(ICacheApi<object?> api)
    {
        var operations = new IInboundEndpointCacheOperationsCreateExpectations<object?>();
        _ = operations.Setups.ForCache(CacheName).ReturnValue(api);
        var ownership = new INodeOwnershipResolverCreateExpectations();
        _ = ownership.Setups.SelfNodeId.Gets().ReturnValue(Self);
        _ = ownership.Setups.GetOwner(Arg.Any<string>(), Arg.Any<string>()).ReturnValue(Self);
        var invocation = new IRemoteInvocationStateCreateExpectations();
        _ = invocation.Setups.IsInternalOwnerInvocation.Gets().ReturnValue(false);
        var store = new RpcMutationIdempotencyStore(new IdempotencyOptions(), Self, new IdempotencyMetrics(_testMeter), TimeProvider.System);

        return new SquirixServiceAdapter<object?>(
            operations.Instance(),
            OwnerRouters.Static(ownership.Instance(), invocation.Instance(), Self),
            new OwnerRpcForwarder(
                new IServerClientPoolCreateExpectations().Instance(),
                new IBackpressureGateCreateExpectations().Instance(),
                new IBackpressureClientIdResolverCreateExpectations().Instance(),
                RingAgreements.Create()),
            new RpcMutationIdempotencyCoordinator(store, NullLogger<RpcMutationIdempotencyCoordinator>.Instance),
            TimeProvider.System);
    }
}
