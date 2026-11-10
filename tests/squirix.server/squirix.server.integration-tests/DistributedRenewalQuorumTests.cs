using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Exercises distributed renewal when the owner cannot confirm a majority.</summary>
public sealed class DistributedRenewalQuorumTests : NodeIntegrationTestBase
{
    private const string OwnerId = "node-a";
    private const string CacheName = "renewal-quorum";

    /// <summary>The minority refuses a sliding Get or Refresh without changing the deadline or appending a renewal.</summary>
    /// <param name="refresh">Whether to call Refresh rather than Get.</param>
    /// <param name="expired">Whether the entry reaches its deadline in the minority.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task MinorityRefusesRenewal(bool refresh, bool expired, CancellationToken cancellationToken)
    {
        var clock = new ShiftableClock();
        var options = WipedCluster.Options(CacheName, true, services: services => services.Replace(ServiceDescriptor.Singleton<TimeProvider>(clock)));
        await using var cluster = await StartClusterAsync(OwnerId, "node-b", "node-c", options, cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster[OwnerId];
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        using var channel = CreateGrpcChannel(owner.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        _ = await SeedAsync(client, key, cancellationToken);
        ILocalCacheReadOperations<object?> local = owner.GetRequiredService<PhysicalCache<object?>>();
        var cacheKey = new CacheKey(CacheName, key);
        var original = local.GetValueWithDeadline(cacheKey, cancellationToken);
        _ = await Assert.That(original.Found).IsTrue();
        _ = await Assert.That(original.ExpiresUtc).IsNotNull();

        await cluster.StopNodeAsync("node-b");
        await cluster.StopNodeAsync("node-c");
        var log = WipedCluster.Log(owner, OwnerId);
        var before = await log.GetStatusAsync(cancellationToken);
        if (expired)
        {
            clock.Advance(TimeSpan.FromMinutes(2));
            var ownerNow = owner.GetRequiredService<TimeProvider>().GetUtcNow().UtcDateTime;
            _ = await Assert.That(ownerNow >= original.ExpiresUtc).IsTrue();
        }

        var attempt = AccessAsync(client, key, refresh, cancellationToken);
        var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempt, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
        var after = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(after.CommitIndex).IsEqualTo(before.CommitIndex);
        if (!expired)
        {
            _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex);
            _ = await Assert.That(local.GetValueWithDeadline(cacheKey, cancellationToken).ExpiresUtc).IsEqualTo(original.ExpiresUtc);
        }
    }

    /// <summary>A renewal durably appended on the leader without follower acknowledgement has an unknown outcome.</summary>
    /// <param name="refresh">Whether to call Refresh rather than Get.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RenewalAfterLocalAppendCanBeUnknown(bool refresh, CancellationToken cancellationToken)
    {
        using var armed = new CancellationTokenSource();
        var parked = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var gateway = ParkedAppendGateway.CreateConfigure(OwnerId, _ => armed.IsCancellationRequested, ParkAsync);
        await using var cluster = await StartClusterAsync(OwnerId, "node-b", "node-c", WipedCluster.Options(CacheName, true, services: gateway), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster[OwnerId];
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        using var channel = CreateGrpcChannel(owner.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        _ = await SeedAsync(client, key, cancellationToken);
        var log = WipedCluster.Log(owner, OwnerId);
        var before = await log.GetStatusAsync(cancellationToken);
        await armed.CancelAsync();
        try
        {
            var attempt = AccessAsync(client, key, refresh, cancellationToken);
            await parked.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            var error = await NodeAsyncAssert.ThrowsAsync<RpcException>(attempt, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            _ = await Assert.That(error.StatusCode).IsEqualTo(StatusCode.Unavailable);
            _ = await Assert.That(error.Status.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
            var after = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That(after.LastLogIndex).IsEqualTo(before.LastLogIndex + 1);
            _ = await Assert.That(after.CommitIndex).IsEqualTo(before.CommitIndex);
        }
        finally
        {
            _ = release.TrySetResult();
        }

        Task ParkAsync(CancellationToken token)
        {
            _ = parked.TrySetResult();
            return release.Task.WaitAsync(token);
        }
    }

    /// <summary>A write submitted during an in-flight renewal wins after the renewal and preserves its own state.</summary>
    /// <param name="remove">Whether the competing mutation removes the key.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ConcurrentMutationDoesNotMixRenewalState(bool remove, CancellationToken cancellationToken)
    {
        var firstAppend = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var armed = new CancellationTokenSource();
        var gateway = ParkedAppendGateway.CreateConfigure(OwnerId, _ => armed.IsCancellationRequested, ParkAsync);
        await using var cluster = await StartClusterAsync(OwnerId, "node-b", "node-c", WipedCluster.Options("renewal-race", true, services: gateway), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster[OwnerId];
        var key = owner.FindKeyOwnedBy(CacheName, OwnerId);
        using var channel = CreateGrpcChannel(owner.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        _ = await SeedAsync(client, key, cancellationToken);
        var log = WipedCluster.Log(owner, OwnerId);
        var before = (await log.GetStatusAsync(cancellationToken)).CommitIndex;
        await armed.CancelAsync();
        Task mutation;
        var access = new GetDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") };
        var competingId = Guid.NewGuid().ToString("N");
        var pending = client.GetDistributedAsync(access, cancellationToken: cancellationToken).ResponseAsync;
        try
        {
            await firstAppend.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
            mutation = SubmitMutationAsync(client, key, competingId, remove, cancellationToken);
            var queue = owner.GetRequiredService<ReplicaGroupCommitters>().For(OwnerId).Combiner;
            await (Queue: queue, OperationId: competingId).WaitUntilValueAsync(
                static (state, _) => ValueTask.FromResult(string.Equals(state.Queue.PeekLive()?.OperationId, state.OperationId, StringComparison.Ordinal)),
                TimeSpan.FromSeconds(10),
                TimeProvider.System,
                cancellationToken);
        }
        finally
        {
            _ = release.TrySetResult();
        }

        var original = await pending;
        await mutation;
        await AssertMutationOrderAsync(log, before, access.OperationId, competingId, cancellationToken);
        await AssertFinalStateAsync(client, access, original, remove, cancellationToken);

        Task ParkAsync(CancellationToken token)
        {
            _ = firstAppend.TrySetResult();
            return release.Task.WaitAsync(token);
        }
    }

    private static Task SubmitMutationAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, string operationId, bool remove, CancellationToken cancellationToken) =>
        remove
            ? client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken).ResponseAsync
            : client.SetDistributedAsync(new SetDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId, Value = ByteString.CopyFrom(7) }, cancellationToken: cancellationToken).ResponseAsync;

    private static async Task AssertMutationOrderAsync(IFollowerLog log, ulong before, string accessId, string mutationId, CancellationToken cancellationToken)
    {
        var entries = await log.GetCommittedEntriesAsync(before, 2, cancellationToken);
        _ = await Assert.That(entries.Count).IsEqualTo(2);
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(entries[0].Payload)?.OperationId).IsEqualTo(accessId);
        _ = await Assert.That(ReplicaLogCodec.DecodeBorrowed(entries[1].Payload)?.OperationId).IsEqualTo(mutationId);
    }

    private static async Task AssertFinalStateAsync(SquirixCacheService.SquirixCacheServiceClient client, GetDistributedAsyncRequest access, GetDistributedAsyncResponse original, bool remove, CancellationToken cancellationToken)
    {
        _ = await Assert.That(original.Found).IsTrue();
        _ = await Assert.That(original.Value).IsEqualTo(ByteString.CopyFrom(0, 255));
        var replay = await client.GetDistributedAsync(access, cancellationToken: cancellationToken);
        _ = await Assert.That(replay).IsEqualTo(original);
        var fresh = access.Clone();
        fresh.OperationId = Guid.NewGuid().ToString("N");
        var current = await client.GetDistributedAsync(fresh, cancellationToken: cancellationToken);
        _ = await Assert.That(current.Found).IsEqualTo(!remove);
        _ = await Assert.That(current.Value).IsEqualTo(remove ? ByteString.Empty : ByteString.CopyFrom(7));
    }

    private static Task<SetAsyncResponse> SeedAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, CancellationToken cancellationToken)
    {
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            Value = ByteString.CopyFrom(0, 255),
            SlidingTicks = TimeSpan.FromMinutes(2).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        return client.SetDistributedAsync(set, cancellationToken: cancellationToken).ResponseAsync;
    }

    private static Task AccessAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, bool refresh, CancellationToken cancellationToken)
    {
        var operationId = Guid.NewGuid().ToString("N");
        return refresh
            ? client.RefreshDistributedAsync(new RefreshDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken).ResponseAsync
            : client.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken).ResponseAsync;
    }

    private sealed class ShiftableClock : TimeProvider
    {
        private long _offset;

        public override DateTimeOffset GetUtcNow() => TimeProvider.System.GetUtcNow().AddTicks(Interlocked.Read(ref _offset));

        internal void Advance(TimeSpan amount) => _ = Interlocked.Add(ref _offset, amount.Ticks);
    }
}
