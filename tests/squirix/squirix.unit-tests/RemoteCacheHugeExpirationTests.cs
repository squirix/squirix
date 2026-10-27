using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Covers the client wire form of a relative expiration beyond the normalized duration range.</summary>
[Immutable]
public sealed class RemoteCacheHugeExpirationTests
{
    private const long FourMillionDaysTicks = 4_000_000L * TimeSpan.TicksPerDay;
    private const string Key = "key-a";
    private const long MaxDurationSeconds = 315_576_000_000L;

    /// <summary>TryAdd sends the largest normalized duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task AddClampsExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = CreatePool(transport, policy);

        _ = await CreateCache(pool).TryAddAsync(Key, 1, new CacheEntryOptions { Expiration = TimeSpan.FromTicks(ticks) }, cancellationToken);

        _ = await Assert.That(transport.Expiration?.Seconds).IsEqualTo(MaxDurationSeconds);
    }

    /// <summary>Set sends the largest normalized duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task SetClampsExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = CreatePool(transport, policy);

        await CreateCache(pool).SetAsync(Key, 1, new CacheEntryOptions { Expiration = TimeSpan.FromTicks(ticks) }, cancellationToken);

        _ = await Assert.That(transport.Expiration?.Seconds).IsEqualTo(MaxDurationSeconds);
    }

    /// <summary>Touch sends the largest normalized duration.</summary>
    /// <param name="ticks">The expiration in ticks.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(long.MaxValue)]
    [Arguments(FourMillionDaysTicks)]
    public async Task TouchClampsExpiration(long ticks, CancellationToken cancellationToken)
    {
        var transport = new CaptureTransport();
        await using var policy = CreatePolicy();
        await using var pool = CreatePool(transport, policy);

        _ = await CreateCache(pool).TouchAsync(Key, TimeSpan.FromTicks(ticks), cancellationToken);

        _ = await Assert.That(transport.Expiration?.Seconds).IsEqualTo(MaxDurationSeconds);
    }

    private static RemoteCache<long> CreateCache(IClientPool pool) => new(
        "demo",
        new EndpointFailover(["node-0"], "node-0", TimeSpan.FromSeconds(30), TimeProvider.System),
        pool,
        RemoteClientSessionFactory.CreateSerializer());

    private static CallPolicy CreatePolicy() => new(TimeProvider.System, TimeSpan.FromSeconds(5), 1, TimeSpan.Zero, TimeSpan.Zero, peer: "huge-expiration");

    private static IClientPool CreatePool(CaptureTransport transport, CallPolicy policy)
    {
        var client = new SquirixCacheService.SquirixCacheServiceClient(transport);
        var poolExpectations = new IClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(client);
        _ = poolExpectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        _ = poolExpectations.Setups.BeginDrain();
        _ = poolExpectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        return poolExpectations.Instance();
    }

    private sealed class CaptureTransport : CallInvoker
    {
        internal Duration? Expiration { get; private set; }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The capture transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The capture transport supports unary calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request) => throw new InvalidOperationException("The capture transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            object response;
            switch (request)
            {
                case SetEntryAsyncRequest set:
                    Expiration = set.Entry.Expiration;
                    response = new SetAsyncResponse();
                    break;
                case TryAddEntryAsyncRequest tryAdd:
                    Expiration = tryAdd.Entry.Expiration;
                    response = new TryAddAsyncResponse { Added = true };
                    break;
                case TouchAsyncRequest touch:
                    Expiration = touch.Expiration;
                    response = new TouchAsyncResponse { Found = true };
                    break;
                default:
                    throw new InvalidOperationException("The capture transport supports set, try-add and touch requests only.");
            }

            var typed = KitThrowHelper.Required(response as TResponse, "The capture transport got an unexpected response type.");
            return new AsyncUnaryCall<TResponse>(
                Task.FromResult(typed),
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The capture transport supports asynchronous calls only.");
    }
}
