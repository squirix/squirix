using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
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

    /// <summary>Retries one immutable RemoteCache mutation within the configured attempt budget and keeps the operation id.</summary>
    [Fact]
    public async Task RetryKeepsOperationIdAndBudgetAsync()
    {
        await using var policy = new CallPolicy(TimeSpan.FromSeconds(1), 3, TimeSpan.Zero, TimeSpan.Zero, peer: "commit-unknown");
        var transport = new UnknownOutcomeTransport();
        var client = new SquirixCacheService.SquirixCacheServiceClient(transport);
        await using var pool = new SingleNodePool(client, policy);
        var cache = new RemoteCache<string>("demo", new EndpointFailover(["node-0"], "node-0"), pool, RemoteClientSessionFactory.CreateSerializer());

        var error = await AsyncAssert.ThrowsAsync<CommitOutcomeUnknownException, bool>(SetAndProjectAsync(cache));

        Assert.Equal(CommitOutcomeUnknownException.StableDetail, error.Message);
        Assert.Same(transport.Failures[^1], error.InnerException);
        Assert.Equal(3, transport.OperationIds.Count);
        var operationId = transport.OperationIds[0];
        Assert.False(string.IsNullOrEmpty(operationId));
        for (var index = 1; index < transport.OperationIds.Count; index++)
            Assert.Equal(operationId, transport.OperationIds[index]);
    }

    private static async ValueTask<bool> SetAndProjectAsync(RemoteCache<string> cache)
    {
        await cache.SetAsync("key-a", "value", cancellationToken: CancellationToken.None).ConfigureAwait(false);
        return true;
    }

    private sealed class UnknownOutcomeTransport : CallInvoker
    {
        internal List<string> OperationIds { get; } = [];

        internal List<RpcException> Failures { get; } = [];

        public override TResponse BlockingUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The commit-unknown transport supports asynchronous calls only.");

        public override AsyncServerStreamingCall<TResponse> AsyncServerStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request) =>
            throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncClientStreamingCall<TRequest, TResponse> AsyncClientStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncDuplexStreamingCall<TRequest, TResponse> AsyncDuplexStreamingCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options) =>
            throw new InvalidOperationException("The commit-unknown transport supports unary calls only.");

        public override AsyncUnaryCall<TResponse> AsyncUnaryCall<TRequest, TResponse>(Method<TRequest, TResponse> method, string? host, CallOptions options, TRequest request)
        {
            _ = method;
            _ = host;
            _ = options;
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

    private sealed class SingleNodePool : IClientPool
    {
        private readonly SquirixCacheService.SquirixCacheServiceClient _client;
        private readonly ICallPolicy _policy;

        internal SingleNodePool(SquirixCacheService.SquirixCacheServiceClient client, ICallPolicy policy)
        {
            _client = client;
            _policy = policy;
        }

        public void BeginDrain()
        {
        }

        public SquirixCacheService.SquirixCacheServiceClient ForNode(string nodeId)
        {
            _ = nodeId;
            return _client;
        }

        public ICallPolicy PolicyFor(string nodeId)
        {
            _ = nodeId;
            return _policy;
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
