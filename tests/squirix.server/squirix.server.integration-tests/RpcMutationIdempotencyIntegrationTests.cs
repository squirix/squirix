using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration coverage for mutating gRPC idempotency on the live adapter path.
/// Uses <see cref="IntegrationSingleNodeFixture"/> to share one server across all tests.
/// </summary>
public sealed class RpcMutationIdempotencyIntegrationTests : NodeIntegrationTestBase, IClassFixture<IntegrationSingleNodeFixture>
{
    private const string MismatchOperationId = "fedcba9876543210fedcba9876543210";

    /// <summary>Valid 32-char hex operation id for idempotency replay tests. Same value as <c language="csharp">IntegrationMutationOpIds.Default</c> but a separate constant for clarity.</summary>
    private const string ReplayOperationId = "0123456789abcdef0123456789abcdef";

    private readonly Uri _uri;

    /// <summary>Initializes a new instance of the <see cref="RpcMutationIdempotencyIntegrationTests"/> class.</summary>
    /// <param name="fixture">Shared single-node fixture.</param>
    public RpcMutationIdempotencyIntegrationTests(IntegrationSingleNodeFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        _uri = fixture.Uri;
    }

    /// <summary>
    /// Verifies mutating RPCs without <c language="csharp">operation_id</c> are rejected at the adapter.
    /// </summary>
    [Fact]
    public async Task EmptyOperationIdReturnsInvalidArgument()
    {
        using var channel = CreateGrpcChannel(_uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    CacheName = "default",
                    Key = "missing-operation-id",
                    Entry = new NodeCacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdRequiredDetail, ex.Status.Detail);
    }

    /// <summary>Verifies a duplicate mutating request replays the cached outcome instead of re-applying.</summary>
    [Fact]
    public async Task RepeatedOperationIdReplaysResponse()
    {
        using var channel = CreateGrpcChannel(_uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ReplayOperationId,
            CacheName = "default",
            Key = "replay-key",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var first = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
        var second = await client.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);

        Assert.True(first.Added);
        Assert.True(second.Added);
    }

    /// <summary>Verifies malformed operation ids are rejected at the adapter.</summary>
    [Fact]
    public async Task BadOperationIdFormatIsInvalidArgument()
    {
        using var channel = CreateGrpcChannel(_uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = "integration-replay-op",
                    CacheName = "default",
                    Key = "invalid-format-operation-id",
                    Entry = new NodeCacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdInvalidFormatDetail, ex.Status.Detail);
    }

    /// <summary>Verifies reusing an operation id with a different mutation fingerprint fails with the stable contract.</summary>
    [Fact]
    public async Task ReuseReturnsFailedPrecondition()
    {
        using var channel = CreateGrpcChannel(_uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        _ = await client.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = MismatchOperationId,
                CacheName = "default",
                Key = "mismatch-a",
                Entry = new NodeCacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
            },
            cancellationToken: DefaultCancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = MismatchOperationId,
                    CacheName = "default",
                    Key = "mismatch-b",
                    Entry = new NodeCacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal(ServerOpIdMismatchException.StableDetail, ex.Status.Detail);
    }

    /// <summary>Verifies over-length operation ids are rejected at the adapter.</summary>
    [Fact]
    public async Task TooLongOperationIdReturnsInvalidArgument()
    {
        using var channel = CreateGrpcChannel(_uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var tooLong = new string('a', RpcMutationContracts.OperationIdLength + 1);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = tooLong,
                    CacheName = "default",
                    Key = "too-long-operation-id",
                    Entry = new NodeCacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.InvalidArgument, ex.StatusCode);
        Assert.Equal(RpcMutationContracts.OperationIdTooLongDetail, ex.Status.Detail);
    }
}
