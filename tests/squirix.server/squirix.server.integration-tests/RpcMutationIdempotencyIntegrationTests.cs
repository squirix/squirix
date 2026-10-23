using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Integration coverage for mutating gRPC idempotency on the live adapter path.
/// Uses <see cref="IntegrationSingleNodeFixture" /> to share one server across all tests.
/// </summary>
public sealed class RpcMutationIdempotencyIntegrationTests : NodeIntegrationTestBase
{
    private const string MismatchOperationId = "fedcba9876543210fedcba9876543210";

    /// <summary>Valid 32-char hex operation id for idempotency replay tests. Same value as <c language="csharp">IntegrationMutationOpIds.Default</c> but a separate constant for clarity.</summary>
    private const string ReplayOperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>Gets the shared single-node fixture injected once per test class.</summary>
    [ClassDataSource<IntegrationSingleNodeFixture>(Shared = SharedType.PerClass)]
    public required IntegrationSingleNodeFixture Fixture { get; init; }

    private Uri Uri => Fixture.Uri;

    /// <summary>Verifies malformed operation ids are rejected at the adapter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BadOperationIdFormatIsInvalidArgument(CancellationToken cancellationToken)
    {
        using var channel = CreateGrpcChannel(Uri);
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
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(RpcMutationContracts.OperationIdInvalidFormatDetail);
    }

    /// <summary>Verifies mutating RPCs without <c language="csharp">operation_id</c> are rejected at the adapter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EmptyOperationIdReturnsInvalidArgument(CancellationToken cancellationToken)
    {
        using var channel = CreateGrpcChannel(Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    CacheName = "default",
                    Key = "missing-operation-id",
                    Entry = new NodeCacheEntry<object?> { Value = "v", Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(RpcMutationContracts.OperationIdRequiredDetail);
    }

    /// <summary>Verifies a duplicate mutating request replays the cached outcome instead of re-applying.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedOperationIdReplaysResponse(CancellationToken cancellationToken)
    {
        using var channel = CreateGrpcChannel(Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new TryAddEntryAsyncRequest
        {
            OperationId = ReplayOperationId,
            CacheName = "default",
            Key = "replay-key",
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToProto(),
        };

        var first = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);
        var second = await client.TryAddEntryAsync(request, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(second.Added).IsTrue();
    }

    /// <summary>Verifies reusing an operation id with a different mutation fingerprint fails with the stable contract.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReuseReturnsFailedPrecondition(CancellationToken cancellationToken)
    {
        using var channel = CreateGrpcChannel(Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        _ = await client.TryAddEntryAsync(
            new TryAddEntryAsyncRequest
            {
                OperationId = MismatchOperationId,
                CacheName = "default",
                Key = "mismatch-a",
                Entry = new NodeCacheEntry<object?> { Value = "a", Version = 1 }.MapToProto(),
            },
            cancellationToken: cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest
                {
                    OperationId = MismatchOperationId,
                    CacheName = "default",
                    Key = "mismatch-b",
                    Entry = new NodeCacheEntry<object?> { Value = "b", Version = 1 }.MapToProto(),
                },
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(ServerOpIdMismatchException.StableDetail);
    }

    /// <summary>Verifies over-length operation ids are rejected at the adapter.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TooLongOperationIdReturnsInvalidArgument(CancellationToken cancellationToken)
    {
        using var channel = CreateGrpcChannel(Uri);
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
                cancellationToken: cancellationToken).ResponseAsync);

        _ = await Assert.That(ex.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
        _ = await Assert.That(ex.Status.Detail).IsEqualTo(RpcMutationContracts.OperationIdTooLongDetail);
    }
}
