using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration coverage for client operation_id propagation across cluster routing hops.
/// Uses <see cref="IntegrationTwoNodeFixture" /> to share two servers across all tests.
/// Each test generates its own unique operation_id to avoid idempotency-store cross-contamination.
/// </summary>
public sealed class CrossNodeOpIdIdempotencyTests : NodeIntegrationTestBase
{
    /// <summary>Gets the shared two-node fixture injected once per test class.</summary>
    [ClassDataSource<IntegrationTwoNodeFixture>(Shared = SharedType.PerClass)]
    public required IntegrationTwoNodeFixture Fixture { get; init; }

    private Uri UriA => Fixture.UriA;

    private Uri UriB => Fixture.UriB;

    /// <summary>Verifies bootstrap-style endpoint failover preserves operation_id and replays the cached mutation outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BootstrapSwitchReplaysSameOperationId(CancellationToken cancellationToken)
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

        using var channelA = CreateGrpcChannel(UriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        _ = await clientA.SetEntryAsync(request, cancellationToken: cancellationToken);

        using var channelB = CreateGrpcChannel(UriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        _ = await clientB.SetEntryAsync(request, cancellationToken: cancellationToken);

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsTrue();
    }

    /// <summary>Verifies a retry with the same operation_id on a different entry node replays the owner outcome instead of double-applying when the key is owned elsewhere.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrossNodeRepeatReplaysCachedResponse(CancellationToken cancellationToken)
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

        using var channelA = CreateGrpcChannel(UriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        var first = await clientA.TryAddEntryAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(first.Added).IsTrue();

        using var channelB = CreateGrpcChannel(UriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var second = await clientB.TryAddEntryAsync(request, cancellationToken: cancellationToken);

        _ = await Assert.That(second.Added).IsTrue();

        var getResponse = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(getResponse.Found).IsTrue();
    }

    /// <summary>Verifies reusing an operation_id with a different fingerprint fails on the owner after entry-node forwarding.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrossNodeReuseReturnsFailedPrecondition(CancellationToken cancellationToken)
    {
        var mismatchOpId = RpcOperationIdentity.New();
        var keyA = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-a");
        var keyB = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "cross-node-mismatch-b");

        using var channelA = CreateGrpcChannel(UriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);

        var r1 = new TryAddEntryAsyncRequest
        {
            OperationId = mismatchOpId,
            CacheName = "default",
            Key = keyA,
            Entry = new NodeCacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
        };
        _ = await clientA.TryAddEntryAsync(r1, cancellationToken: cancellationToken);

        var r2 = new TryAddEntryAsyncRequest
        {
            OperationId = mismatchOpId,
            CacheName = "default",
            Key = keyB,
            Entry = new NodeCacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
        };
        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(clientA.TryAddEntryAsync(r2, cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(ServerOpIdMismatchException.StableDetail);
    }
}
