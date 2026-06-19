using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Node.Services;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests.Reliability;

/// <summary>Integration coverage for mutating gRPC idempotency on the live adapter path.</summary>
public sealed class RpcMutationIdempotencyIntegrationTests : IntegrationTestBase
{
    private const string ValidOperationId = "0123456789abcdef0123456789abcdef";
    private const string MismatchOperationId = "fedcba9876543210fedcba9876543210";

    /// <summary>
    /// Verifies mutating RPCs without <c>operation_id</c> are rejected at the adapter.
    /// </summary>
    [Fact]
    public async Task EmptyOperationIdReturnsInvalidArgument()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node-a", Url = url.AbsoluteUri } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.TrySetAsync(
                new TrySetRequest
                {
                    CacheName = "default",
                    Key = "missing-operation-id",
                    Entry = new CacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>Verifies over-length operation ids are rejected at the adapter.</summary>
    [Fact]
    public async Task TooLongOperationIdReturnsInvalidArgument()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node-a", Url = url.AbsoluteUri } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var tooLong = new string('a', RpcMutationContracts.OperationIdLength + 1);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.TrySetAsync(
                new TrySetRequest
                {
                    OperationId = tooLong,
                    CacheName = "default",
                    Key = "too-long-operation-id",
                    Entry = new CacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdTooLongDetail, ex.Status.Detail);
    }

    /// <summary>Verifies malformed operation ids are rejected at the adapter.</summary>
    [Fact]
    public async Task InvalidFormatOperationIdReturnsInvalidArgument()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node-a", Url = url.AbsoluteUri } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.TrySetAsync(
                new TrySetRequest
                {
                    OperationId = "integration-replay-op",
                    CacheName = "default",
                    Key = "invalid-format-operation-id",
                    Entry = new CacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Verifies a duplicate mutating request replays the cached outcome instead of re-applying.</summary>
    [Fact]
    public async Task IdenticalOperationIdReplaysCachedResponse()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node-a", Url = url.AbsoluteUri } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new TrySetRequest
        {
            OperationId = ValidOperationId,
            CacheName = "default",
            Key = "replay-key",
            Entry = new CacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var first = await client.TrySetAsync(request, cancellationToken: DefaultCancellationToken);
        var second = await client.TrySetAsync(request, cancellationToken: DefaultCancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
    }

    /// <summary>Verifies reusing an operation id with a different mutation fingerprint fails with the stable contract.</summary>
    [Fact]
    public async Task ReusedOperationIdWithDifferentFingerprintReturnsFailedPrecondition()
    {
        var url = GetNextHttpUri();
        var peers = new[] { new Peer { NodeId = "node-a", Url = url.AbsoluteUri } };
        await using var node = await StartNodeAsync(url, peers);

        using var channel = CreateGrpcChannel(url);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        _ = await client.TrySetAsync(
            new TrySetRequest
            {
                OperationId = MismatchOperationId,
                CacheName = "default",
                Key = "mismatch-a",
                Entry = new CacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        var ex = await Assert.ThrowsAsync<RpcException>(async () =>
        {
            _ = await client.TrySetAsync(
                new TrySetRequest
                {
                    OperationId = MismatchOperationId,
                    CacheName = "default",
                    Key = "mismatch-b",
                    Entry = new CacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken);
        });

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal(OperationIdReuseMismatchException.StableDetail, ex.Status.Detail);
    }
}
