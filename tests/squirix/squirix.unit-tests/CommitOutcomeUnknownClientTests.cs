using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Rocks;
using Squirix.Attributes;
using Squirix.Internal;
using Squirix.Internal.Cluster.Reliability;
using Squirix.Internal.Cluster.Transport;
using Squirix.TestKit;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.UnitTests;

/// <summary>Covers the public client contract for ambiguous durable mutation outcomes.</summary>
[Immutable]
public sealed class CommitOutcomeUnknownClientTests
{
    /// <summary>Maps only the stable unavailable detail and preserves the transport failure.</summary>
    [Fact]
    public void MapsStableCodeToPublicException()
    {
        var transport = new RpcException(new Status(StatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));

        var error = Assert.IsType<CommitOutcomeUnknownException>(CommitOutcomeUnknownClassifier.Map(transport));

        Assert.Equal(CommitOutcomeUnknownException.StableDetail, error.Message);
        Assert.Same(transport, error.InnerException);
    }

    /// <summary>Declines to map unrelated unavailable failures and returns null.</summary>
    [Fact]
    public void OtherUnavailableRemainsTransportError()
    {
        var transport = new RpcException(new Status(StatusCode.Unavailable, "peer unavailable"));

        var error = CommitOutcomeUnknownClassifier.Map(transport);

        Assert.Null(error);
    }

    /// <summary>Surfaces an ambiguous outcome immediately without consuming the retry budget, keeping the operation id.</summary>
    [Fact]
    public async Task UnknownOutcomeStopsWithoutRetryingAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "commit-unknown");
        var transport = new UnknownOutcomeTransport();
        var client = new SquirixCacheService.SquirixCacheServiceClient(transport);
        var poolExpectations = new IClientPoolCreateExpectations();
        _ = poolExpectations.Setups.ForNode(Arg.Any<string>()).ReturnValue(client);
        _ = poolExpectations.Setups.PolicyFor(Arg.Any<string>()).ReturnValue(policy);
        _ = poolExpectations.Setups.BeginDrain();
        _ = poolExpectations.Setups.DisposeAsync().ReturnValue(ValueTask.CompletedTask);
        await using var pool = poolExpectations.Instance();
        var cache = new RemoteCache<string>("demo", new EndpointFailover(["node-0"], "node-0"), pool, RemoteClientSessionFactory.CreateSerializer());

        var error = await AsyncAssert.ThrowsAsync<CommitOutcomeUnknownException, bool>(SetAndProjectAsync(cache));

        Assert.Equal(CommitOutcomeUnknownException.StableDetail, error.Message);
        Assert.Same(transport.Failures[^1], error.InnerException);
        var operationId = Assert.Single(transport.OperationIds);
        Assert.False(string.IsNullOrEmpty(operationId));
    }

    private static async ValueTask<bool> SetAndProjectAsync(RemoteCache<string> cache)
    {
        await cache.SetAsync("key-a", "value", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        return true;
    }

    private sealed class UnknownOutcomeTransport : CallInvoker
    {
        internal List<RpcException> Failures { get; } = [];

        internal List<string> OperationIds { get; } = [];

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options) => throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(
            Method<TRequest, TResponse> method,
            string? host,
            CallOptions options,
            TRequest request) => throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            var failure = new RpcException(new Status(StatusCode.Unavailable, CommitOutcomeUnknownException.StableDetail));
            Failures.Add(failure);
            OperationIds.Add(ExtractOperationId(request));
            return new AsyncUnaryCall<TResponse>(
                Task.FromException<TResponse>(failure),
                Task.FromResult(new Metadata()),
                static () => new Status(StatusCode.OK, string.Empty),
                static () => [],
                static () => { });
        }

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The commit-unknown transport supports asynchronous calls only.");

        private static string ExtractOperationId<TRequest>(TRequest request) => request switch
        {
            SetEntryAsyncRequest set => set.OperationId,
            TryAddEntryAsyncRequest add => add.OperationId,
            UpdateAsyncRequest update => update.OperationId,
            GetOrAddAsyncRequest getOrAdd => getOrAdd.OperationId,
            RemoveAsyncRequest remove => remove.OperationId,
            RemoveExpirationAsyncRequest removeExpiration => removeExpiration.OperationId,
            TouchAsyncRequest touch => touch.OperationId,
            _ => string.Empty,
        };
    }
}
