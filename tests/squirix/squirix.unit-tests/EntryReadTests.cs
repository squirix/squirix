using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
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

/// <summary>Covers entry results and expiration metadata after a suspended remote response.</summary>
[Immutable]
public sealed class EntryReadTests
{
    /// <summary>A suspended entry read preserves misses, values and expiration rebasing on the client clock.</summary>
    /// <param name="scenario">Zero for a miss, one for a wire deadline, or two for a remaining lifetime.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task EntryResultKeepsWireMetadata(int scenario)
    {
        var initial = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero);
        var clock = new FakeTimeProvider(initial);
        var wireDeadline = initial.UtcDateTime.AddDays(1);
        var response = new GetEntryAsyncResponse
        {
            Found = scenario != 0,
            Entry = scenario == 0
                ? null
                : new CacheEntryWire
                {
                    Value = new CacheValue { StringValue = "value" },
                    Expiration = Duration.FromTimeSpan(TimeSpan.FromMinutes(2)),
                    ExpiresUtc = Timestamp.FromDateTime(wireDeadline),
                },
            Remaining = scenario == 2 ? Duration.FromTimeSpan(TimeSpan.FromSeconds(30)) : null,
        };
        var release = new TaskCompletionSource<GetEntryAsyncResponse>(TaskCreationOptions.RunContinuationsAsynchronously);
        var client = new SquirixCacheService.SquirixCacheServiceClient(new EntryTransport(release.Task));
        await using var policy = new CallPolicy(clock, TimeSpan.FromHours(2));
        var expectations = new IClientPoolCreateExpectations();
        _ = expectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(new NodeClient(client, policy));
        var cache = new RemoteCache<string>("demo", new EndpointFailover(["node-a"], "node-a", TimeSpan.FromHours(1), clock), expectations.Instance(), RemoteClientSessionFactory.CreateSerializer());

        var operation = cache.GetEntryAsync("key-a", CancellationToken.None);
        var pending = !operation.IsCompleted;
        clock.Advance(TimeSpan.FromMinutes(1));
        _ = release.TrySetResult(response);
        var result = await operation.WaitAsync(TimeSpan.FromSeconds(5), TimeProvider.System, CancellationToken.None);

        _ = await Assert.That(pending).IsTrue();
        _ = await Assert.That(result.Found).IsEqualTo(scenario != 0);
        if (scenario == 0)
        {
            _ = await Assert.That(result.Entry).IsNull();
            return;
        }

        _ = await Assert.That(result.Value).IsEqualTo("value");
        _ = await Assert.That(result.Entry!.Expiration).IsEqualTo(TimeSpan.FromMinutes(2));
        _ = await Assert.That(result.ExpiresUtc).IsEqualTo(scenario == 2 ? clock.GetUtcNow().UtcDateTime.AddSeconds(30) : wireDeadline);
    }

    private sealed class EntryTransport : CallInvoker
    {
        private readonly Task<GetEntryAsyncResponse> _response;

        internal EntryTransport(Task<GetEntryAsyncResponse> response)
        {
            _response = response;
        }

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("The entry transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("The entry transport supports unary calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The entry transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) => new(
            ReadResponseAsync<TResponse>(_response, options.CancellationToken),
            Task.FromResult(new Metadata()),
            static () => new Status(StatusCode.OK, string.Empty),
            static () => [],
            static () => { });

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The entry transport supports asynchronous calls only.");

        private static async Task<TResponse> ReadResponseAsync<TResponse>(Task<GetEntryAsyncResponse> response, CancellationToken cancellationToken)
            where TResponse : class => KitThrowHelper.Required(await response.WaitAsync(cancellationToken).ConfigureAwait(false) as TResponse, "The entry transport got an unexpected response type.");
    }
}
