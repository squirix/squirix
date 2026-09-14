using System;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration coverage for client operation_id propagation across cluster routing hops.
/// Uses <see cref="IntegrationTwoNodeFixture"/> to share two servers across all tests.
/// Each test generates its own unique operation_id to avoid idempotency-store cross-contamination.
/// </summary>
public sealed class CrossNodeOpIdIdempotencyTests : NodeIntegrationTestBase, IClassFixture<IntegrationTwoNodeFixture>
{
    private readonly Uri _uriA;
    private readonly Uri _uriB;

    /// <summary>Initializes a new instance of the <see cref="CrossNodeOpIdIdempotencyTests"/> class.</summary>
    /// <param name="fixture">Shared two-node fixture.</param>
    public CrossNodeOpIdIdempotencyTests(IntegrationTwoNodeFixture fixture)
    {
        ArgumentNullException.ThrowIfNull(fixture);
        _uriA = fixture.UriA;
        _uriB = fixture.UriB;
    }

    /// <summary>Verifies bootstrap-style endpoint failover preserves operation_id and replays the cached mutation outcome.</summary>
    [Fact]
    public async Task BootstrapSwitchReplaysSameOperationId()
    {
        var opId = RpcOperationIdentity.New();
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-a", "bootstrap-idempotency");
        var request = new SetEntryAsyncRequest
        {
            OperationId = opId,
            CacheName = "default",
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "bootstrap-value", Version = 1 }.MapToProto(),
        };

        using var channelA = CreateGrpcChannel(_uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        _ = await clientA.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);

        using var channelB = CreateGrpcChannel(_uriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        _ = await clientB.SetEntryAsync(request, cancellationToken: DefaultCancellationToken);

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
    }

    /// <summary>Verifies a retry with the same operation_id on a different entry node replays the owner outcome instead of double-applying when the key is owned elsewhere.</summary>
    [Fact]
    public async Task CrossNodeRepeatReplaysCachedResponse()
    {
        var operationId = RpcOperationIdentity.New();
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cross-node-idempotency");
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = operationId,
            CacheName = "default",
            Key = key,
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        using var channelA = CreateGrpcChannel(_uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var first = await clientA.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);
        Assert.True(first.Added);

        using var channelB = CreateGrpcChannel(_uriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var second = await clientB.TryAddEntryAsync(request, cancellationToken: DefaultCancellationToken);

        Assert.True(second.Added);

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: DefaultCancellationToken);
        Assert.True(getResponse.Found);
    }

    /// <summary>Verifies reusing an operation_id with a different fingerprint fails on the owner after entry-node forwarding.</summary>
    [Fact]
    public async Task CrossNodeReuseReturnsFailedPrecondition()
    {
        var mismatchOpId = RpcOperationIdentity.New();
        var keyA = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-a");
        var keyB = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-b");

        using var channelA = CreateGrpcChannel(_uriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);

        var r1 = new TryAddEntryAsyncRequest
        {
            OperationId = mismatchOpId,
            CacheName = "default",
            Key = keyA,
            Entry = new NodeCacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
        };
        _ = await clientA.TryAddEntryAsync(r1, cancellationToken: DefaultCancellationToken);

        var r2 = new TryAddEntryAsyncRequest
        {
            OperationId = mismatchOpId,
            CacheName = "default",
            Key = keyB,
            Entry = new NodeCacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
        };
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(clientA.TryAddEntryAsync(r2, cancellationToken: DefaultCancellationToken).ResponseAsync);

        Assert.Equal(StatusCode.FailedPrecondition, ex.StatusCode);
        Assert.Equal(ServerOpIdMismatchException.StableDetail, ex.Status.Detail);
    }
}
