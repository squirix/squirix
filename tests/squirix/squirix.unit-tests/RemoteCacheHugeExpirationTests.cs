using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the client wire form of a relative expiration up to TimeSpan.MaxValue.</summary>
[Immutable]
public sealed class RemoteCacheHugeExpirationTests
{
    private const long FourMillionDaysTicks = 4_000_000L * TimeSpan.TicksPerDay;
    private const string Key = "key-a";

    /// <summary>TryAdd sends the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task AddPreservesExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = SingleNodeClientPool.Create(transport.InvokeAsync, policy);

        _ = await CreateCache(pool).TryAddAsync(Key, 1, new CacheEntryOptions { Expiration = TimeSpan.FromTicks(ticks) }, cancellationToken);

        _ = await Assert.That(transport.Expiration?.Ticks).IsEqualTo(ticks);
    }

    /// <summary>Set sends the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task SetPreservesExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = SingleNodeClientPool.Create(transport.InvokeAsync, policy);

        await CreateCache(pool).SetAsync(Key, 1, new CacheEntryOptions { Expiration = TimeSpan.FromTicks(ticks) }, cancellationToken);

        _ = await Assert.That(transport.Expiration?.Ticks).IsEqualTo(ticks);
    }

    /// <summary>Touch sends the exact duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task TouchPreservesExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = SingleNodeClientPool.Create(transport.InvokeAsync, policy);

        _ = await CreateCache(pool).TouchAsync(Key, TimeSpan.FromTicks(ticks), cancellationToken);

        _ = await Assert.That(transport.Expiration?.Ticks).IsEqualTo(ticks);
    }

    private static RemoteCache<long> CreateCache(IClientPool pool) => new(
        "demo",
        new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System),
        pool,
        RemoteClientSessionFactory.CreateSerializer());

    private static CallPolicy CreatePolicy() => new(TimeProvider.System, TimeSpan.FromSeconds(5), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "huge-expiration");

    private sealed class CaptureTransport
    {
        internal TimeSpan? Expiration { get; private set; }

        internal Func<BinaryOperation, WireMessage, IWireParser, BinaryCallOptions, Task<WireMessage>> InvokeAsync =>
            (_, request, _, _) => HandleAsync(request);

        private Task<WireMessage> HandleAsync(WireMessage request)
        {
            WireMessage response;
            switch (request)
            {
                case SetEntryAsyncRequest set:
                    Expiration = set.Entry?.Expiration;
                    response = new SetAsyncResponse();
                    break;
                case TryAddEntryAsyncRequest tryAdd:
                    Expiration = tryAdd.Entry?.Expiration;
                    response = new TryAddAsyncResponse { Added = true };
                    break;
                case TouchAsyncRequest touch:
                    Expiration = touch.Expiration;
                    response = new TouchAsyncResponse { Found = true };
                    break;
                default:
                    throw new InvalidOperationException("The capture transport supports set, try-add and touch requests only.");
            }

            return Task.FromResult(response);
        }
    }
}
