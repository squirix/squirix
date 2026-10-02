using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
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
    /// <summary>The single-key mutations that clients send to any node.</summary>
    public enum ForwardedMutation
    {
        /// <summary>Gets the value, adding the entry when absent.</summary>
        GetOrAdd = 0,

        /// <summary>Removes the entry.</summary>
        Remove = 1,

        /// <summary>Removes the expiration of the entry.</summary>
        RemoveExpiration = 2,

        /// <summary>Sets the entry.</summary>
        SetEntry = 3,

        /// <summary>Sets a new expiration on the entry.</summary>
        Touch = 4,

        /// <summary>Adds the entry when absent.</summary>
        TryAddEntry = 5,

        /// <summary>Replaces the value of the entry.</summary>
        Update = 6,
    }

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

    /// <summary>Every mutation sent to the key owner first and replayed through the other node answers identically and takes effect once.</summary>
    /// <param name="mutation">The mutation under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(ForwardedMutation.GetOrAdd)]
    [Arguments(ForwardedMutation.Remove)]
    [Arguments(ForwardedMutation.RemoveExpiration)]
    [Arguments(ForwardedMutation.SetEntry)]
    [Arguments(ForwardedMutation.Touch)]
    [Arguments(ForwardedMutation.TryAddEntry)]
    [Arguments(ForwardedMutation.Update)]
    public Task OwnerThenNonOwnerReplaysOnce(ForwardedMutation mutation, CancellationToken cancellationToken) => AssertReplayOnceAsync(mutation, false, cancellationToken);

    /// <summary>Every mutation sent to a non-owner first (and forwarded) and replayed on the key owner answers identically and takes effect once.</summary>
    /// <param name="mutation">The mutation under test.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(ForwardedMutation.GetOrAdd)]
    [Arguments(ForwardedMutation.Remove)]
    [Arguments(ForwardedMutation.RemoveExpiration)]
    [Arguments(ForwardedMutation.SetEntry)]
    [Arguments(ForwardedMutation.Touch)]
    [Arguments(ForwardedMutation.TryAddEntry)]
    [Arguments(ForwardedMutation.Update)]
    public Task NonOwnerThenOwnerReplaysOnce(ForwardedMutation mutation, CancellationToken cancellationToken) => AssertReplayOnceAsync(mutation, true, cancellationToken);

    /// <summary>Reads sent to a non-owner are forwarded to the key owner and return what the owner holds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ForwardedReadsReturnOwnerState(CancellationToken cancellationToken)
    {
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", "forwarded-reads");
        using var channelA = CreateGrpcChannel(UriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        using var channelB = CreateGrpcChannel(UriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        _ = await clientB.SetEntryAsync(CreateSet(key, "owner-value", TimeSpan.FromHours(1)), cancellationToken: cancellationToken);

        var value = await clientA.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        var entry = await clientA.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        var expiration = await clientA.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);

        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value.StringValue).IsEqualTo("owner-value");
        _ = await Assert.That(entry.Found).IsTrue();
        _ = await Assert.That(expiration.HasExpiration).IsTrue();
    }

    private static SetEntryAsyncRequest CreateSet(string key, string value, TimeSpan? expiration = null) => new()
    {
        OperationId = RpcOperationIdentity.New(),
        CacheName = "default",
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1, Expiration = expiration }.MapToProto(),
    };

    private static bool HasRecord(ITestNodeHost node, string operationId)
    {
        var records = new List<PersistedIdempotencyRecord>();
        node.Services.GetRequiredService<IIdempotencySnapshotExporter>().ExportSnapshot(records, DateTime.UtcNow);
        foreach (var record in CollectionsMarshal.AsSpan(records))
        {
            if (string.Equals(record.OperationId, operationId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private static async Task<ByteString> SendAsync(ForwardedMutation mutation, SquirixCacheService.SquirixCacheServiceClient client, string key, string operationId, CancellationToken cancellationToken)
    {
        var entry = new NodeCacheEntry<object?> { Value = "mutated", Version = 1 }.MapToProto();
        return mutation switch
        {
            ForwardedMutation.GetOrAdd => (await client.GetOrAddAsync(new GetOrAddAsyncRequest { OperationId = operationId, CacheName = "default", Key = key, Entry = entry }, cancellationToken: cancellationToken))
               .ToByteString(),
            ForwardedMutation.Remove => (await client.RemoveAsync(new RemoveAsyncRequest { OperationId = operationId, CacheName = "default", Key = key }, cancellationToken: cancellationToken)).ToByteString(),
            ForwardedMutation.RemoveExpiration => (await client.RemoveExpirationAsync(
                new RemoveExpirationAsyncRequest { OperationId = operationId, CacheName = "default", Key = key },
                cancellationToken: cancellationToken)).ToByteString(),
            ForwardedMutation.SetEntry => (await client.SetEntryAsync(new SetEntryAsyncRequest { OperationId = operationId, CacheName = "default", Key = key, Entry = entry }, cancellationToken: cancellationToken))
               .ToByteString(),
            ForwardedMutation.Touch => (await client.TouchAsync(
                new TouchAsyncRequest { OperationId = operationId, CacheName = "default", Key = key, Expiration = Google.Protobuf.WellKnownTypes.Duration.FromTimeSpan(TimeSpan.FromHours(2)) },
                cancellationToken: cancellationToken)).ToByteString(),
            ForwardedMutation.TryAddEntry => (await client.TryAddEntryAsync(
                new TryAddEntryAsyncRequest { OperationId = operationId, CacheName = "default", Key = key, Entry = entry },
                cancellationToken: cancellationToken)).ToByteString(),
            ForwardedMutation.Update => (await client.UpdateAsync(new UpdateAsyncRequest { OperationId = operationId, CacheName = "default", Key = key, Entry = entry }, cancellationToken: cancellationToken))
               .ToByteString(),
            _ => throw new ArgumentOutOfRangeException(nameof(mutation), mutation, "Unsupported mutation."),
        };
    }

    private async Task AssertReplayOnceAsync(ForwardedMutation mutation, bool nonOwnerFirst, CancellationToken cancellationToken)
    {
        var key = TestKeyOwnerHelper.TwoNode.FindKeyOwnedBy("default", "node-b", $"replay-once-{mutation}-{nonOwnerFirst}");
        using var channelA = CreateGrpcChannel(UriA);
        var clientA = new SquirixCacheService.SquirixCacheServiceClient(channelA);
        using var channelB = CreateGrpcChannel(UriB);
        var clientB = new SquirixCacheService.SquirixCacheServiceClient(channelB);
        var first = nonOwnerFirst ? clientA : clientB;
        var second = nonOwnerFirst ? clientB : clientA;
        var addsTheKey = mutation is ForwardedMutation.GetOrAdd or ForwardedMutation.TryAddEntry;
        if (!addsTheKey)
            _ = await clientB.SetEntryAsync(CreateSet(key, "initial", TimeSpan.FromHours(1)), cancellationToken: cancellationToken);

        var operationId = RpcOperationIdentity.New();
        var firstResponse = await SendAsync(mutation, first, key, operationId, cancellationToken);

        // A change made after the first attempt: a re-executed mutation would undo it or answer differently.
        _ = await clientB.SetEntryAsync(CreateSet(key, "intervening"), cancellationToken: cancellationToken);
        var secondResponse = await SendAsync(mutation, second, key, operationId, cancellationToken);

        _ = await Assert.That(secondResponse).IsEqualTo(firstResponse);
        var value = await clientB.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        var expiration = await clientB.GetExpirationAsync(new GetExpirationAsyncRequest { CacheName = "default", Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(value.Found).IsTrue();
        _ = await Assert.That(value.Value.StringValue).IsEqualTo("intervening");
        _ = await Assert.That(expiration.HasExpiration).IsFalse();
        _ = await Assert.That(HasRecord(Fixture.NodeB, operationId)).IsTrue();
        _ = await Assert.That(HasRecord(Fixture.NodeA, operationId)).IsFalse();
    }
}
