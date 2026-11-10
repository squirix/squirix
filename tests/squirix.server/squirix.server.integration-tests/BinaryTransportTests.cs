using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Exercises raw binary transport through durability and request validation.</summary>
public sealed class BinaryTransportTests : NodeIntegrationTestBase
{
    private const string NodeId = "binary-node";
    private const string CacheName = "binary";

    /// <summary>Binary values survive unclean restart with journal-only or snapshot-plus-tail recovery.</summary>
    /// <param name="withSnapshot">Whether to publish a snapshot before writing the journal tail.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task BinarySurvivesUncleanRecovery(bool withSnapshot, CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = true }, cancellationToken);
        var node = cluster[NodeId];
        await node.GetRequiredService<SnapshotTriggerService<object?>>().StopAsync(cancellationToken);
        byte[] expected = [0, 255, 128, 34, 0];
        using (var channel = CreateGrpcChannel(node.Uri))
        {
            var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
            _ = await client.SetEntryAsync(Request("snapshot", expected), cancellationToken: cancellationToken);
            if (withSnapshot)
            {
                var snapshot = node.GetRequiredService<Coordinator>();
                _ = await Assert.That(await snapshot.SnapshotAsync(node.GetRequiredService<IJournalCoordinator>(), cancellationToken)).IsTrue();
            }

            _ = await client.SetEntryAsync(Request("tail", expected), cancellationToken: cancellationToken);
            _ = await client.SetEntryAsync(Request("empty", []), cancellationToken: cancellationToken);
            var manifest = await node.GetRequiredService<Ledger>().ReadCurrentOrDefaultAsync(cancellationToken);
            _ = await Assert.That(manifest.LastSnapshot != null).IsEqualTo(withSnapshot);
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
        var restarted = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
        using var recoveredChannel = CreateGrpcChannel(restarted.Uri);
        var recovered = new SquirixCacheService.SquirixCacheServiceClient(recoveredChannel);
        await AssertValueAsync(recovered, "snapshot", expected, cancellationToken);
        await AssertValueAsync(recovered, "tail", expected, cancellationToken);
        await AssertValueAsync(recovered, "empty", [], cancellationToken);
    }

    /// <summary>The encoded entry limit accepts the exact binary boundary and rejects one extra byte without overwriting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinarySizeBoundaryIsAtomic(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, cancellationToken: cancellationToken);
        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);

        // Two flags, Int64 version, UInt16 tag count, byte kind and Int32 byte length.
        var maximum = new byte[EntryLimits.MaxEntrySizeBytes - 17];
        maximum[^1] = 255;
        _ = await client.SetEntryAsync(Request("limit", maximum), cancellationToken: cancellationToken);
        var tooLarge = new byte[maximum.Length + 1];
        var failure = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.SetEntryAsync(Request("limit", tooLarge), cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        await AssertValueAsync(client, "limit", maximum, cancellationToken);
    }

    /// <summary>Forwarded binary writes replicate and a wiped follower receives the same bytes during state transfer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinaryReplicatesAndRepairs(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("binary-repair", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var key = cluster["node-a"].FindKeyOwnedBy(CacheName, "node-a");
        byte[] expected = [0, 255, 128, 0, 34];
        using var channel = CreateGrpcChannel(cluster["node-b"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        _ = await client.SetEntryAsync(Request(key, expected), cancellationToken: cancellationToken);
        foreach (var id in WipedCluster.Nodes)
        {
            var local = cluster[id].GetRequiredService<PhysicalCache<object?>>();
            await (Cache: local, Key: new CacheKey(CacheName, key), Expected: expected).WaitUntilValueAsync(
                static (state, token) => ValueTask.FromResult(state.Cache.GetValue(state.Key, token).Value is byte[] bytes && bytes.AsSpan().SequenceEqual(state.Expected)),
                WipedCluster.Bound,
                TimeProvider.System,
                cancellationToken);
        }

        var repaired = await cluster.RestartWipedNodeAsync("node-c", null, cancellationToken);
        _ = await WipedCluster.AwaitRepairedCopyAsync(repaired, "node-a", "node-a", cancellationToken);
        var recovered = repaired.GetRequiredService<PhysicalCache<object?>>().GetValue(new CacheKey(CacheName, key), cancellationToken);
        var value = (await Assert.That(recovered.Value).IsTypeOf<byte[]>())!;
        _ = await Assert.That(value.AsSpan().SequenceEqual(expected)).IsTrue();
        await AssertValueAsync(client, key, expected, cancellationToken);
    }

    /// <summary>Set stores the exact deadline and replays without extending it after a restart.</summary>
    /// <param name="persistent">Whether the node uses a journal and can recover the entry.</param>
    /// <param name="relative">Whether a relative TTL is earlier than the absolute cap.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task DistributedSetPinsDeadlineOnReplay(bool persistent, bool relative, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1).AddTicks(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        if (persistent)
            await node.GetRequiredService<SnapshotTriggerService<object?>>().StopAsync(cancellationToken);

        var key = new CacheKey(CacheName, "policy");
        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "policy",
            Value = ByteString.CopyFrom(255, 0, 128),
            SlidingTicks = TimeSpan.FromSeconds(20).Ticks + 1,
            ExpiresAtUnixMs = clock.GetUtcNow().AddSeconds(30).ToUnixTimeMilliseconds(),
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var expectedPolicy = request.Clone();
        var initial = DateTime.UnixEpoch.AddDays(1).AddSeconds(relative ? 10 : 20);
        if (relative)
        {
            request.TtlTicks = TimeSpan.FromSeconds(10).Ticks;
            expectedPolicy.ExpiresAtUnixMs = new DateTimeOffset(initial).ToUnixTimeMilliseconds();
        }

        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<object?>>(), key, expectedPolicy, initial, cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(2));
        if (persistent)
        {
            await node.AbruptShutdownAsync();
            await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
            _ = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
        }

        using var replayChannel = CreateGrpcChannel(cluster[NodeId].Uri);
        var replay = new SquirixCacheService.SquirixCacheServiceClient(replayChannel);
        _ = await replay.SetDistributedAsync(request, cancellationToken: cancellationToken);
        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<object?>>(), key, expectedPolicy, initial, cancellationToken);
        var changed = request.Clone();
        if (relative)
            changed.TtlTicks++;
        else
            changed.SlidingTicks++;
        var conflict = await NodeAsyncAssert.ThrowsAsync<RpcException>(replay.SetDistributedAsync(changed, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(conflict.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        await AssertInvalidPolicyRejectedAsync(replay, request, cancellationToken);
        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<object?>>(), key, expectedPolicy, initial, cancellationToken);
        await AssertPolicyReplacementAsync(replay, request, clock, cancellationToken);
    }

    /// <summary>A forwarded Set pins one deadline on every replica and a retry preserves it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DistributedSetReplicatesPinnedDeadline(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("distributed-set", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var key = cluster["node-a"].FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateGrpcChannel(cluster["node-b"].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            Value = ByteString.CopyFrom(0, 255),
            SlidingTicks = TimeSpan.FromMinutes(5).Ticks,
            ExpiresAtUnixMs = TimeProvider.System.GetUtcNow().AddMinutes(10).ToUnixTimeMilliseconds(),
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        var cacheKey = new CacheKey(CacheName, key);
        var leader = cluster["node-a"].GetRequiredService<ILocalCacheReadOperations<object?>>();
        var deadline = leader.GetValueWithDeadline(cacheKey, cancellationToken).ExpiresUtc;
        _ = await Assert.That(deadline).IsNotNull();
        foreach (var id in WipedCluster.Nodes)
        {
            var local = cluster[id].GetRequiredService<ILocalCacheReadOperations<object?>>();
            await (Cache: local, Key: cacheKey).WaitUntilValueAsync(
                static (state, token) => ValueTask.FromResult(state.Cache.GetValueWithDeadline(state.Key, token).Found),
                WipedCluster.Bound,
                TimeProvider.System,
                cancellationToken);
            await AssertDistributedEntryAsync(local, cacheKey, request, deadline.Value, cancellationToken);
        }

        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        await AssertDistributedEntryAsync(leader, cacheKey, request, deadline.Value, cancellationToken);
        var changed = request.Clone();
        changed.ExpiresAtUnixMs++;
        var conflict = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.SetDistributedAsync(changed, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(conflict.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
    }

    /// <summary>A new leader replays a renewal before interpreting a replacement at the same key.</summary>
    /// <param name="refresh">Whether to exercise the payload-free refresh outcome.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DistributedAccessReplaysOriginalResult(bool refresh, CancellationToken cancellationToken)
    {
        var loss = new LostForwardResponseProbe();
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("distributed-access", true, services: loss.Register), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateGrpcChannel(owner.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var cap = TimeProvider.System.GetUtcNow().AddMinutes(2).ToUnixTimeMilliseconds();
        var policy = new DistributedCachePolicy(cap, TimeSpan.FromMinutes(5).Ticks);
        byte[] payload = [255, 0, 128];
        var seed = Request(key, policy.Encode(payload));
        seed.Entry.ExpiresUtc = Timestamp.FromDateTimeOffset(DateTimeOffset.FromUnixTimeMilliseconds(cap).AddMinutes(-1));
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);

        var operationId = Guid.NewGuid().ToString("N");
        loss.Lose(key, static () => Task.CompletedTask);
        using var entryChannel = CreateGrpcChannel(cluster["node-b"].Uri);
        var entryClient = new SquirixCacheService.SquirixCacheServiceClient(entryChannel);
        var first = AccessRpcAsync(entryClient, operationId, key, refresh, cancellationToken);
        var lost = await NodeAsyncAssert.ThrowsAsync<RpcException>(first, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(lost.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(loss.OwnerExecutions(key)).IsEqualTo(1);

        var stored = owner.GetRequiredService<ILocalCacheReadOperations<object?>>();
        var cacheKey = new CacheKey(CacheName, key);
        _ = await Assert.That(stored.GetValueWithDeadline(cacheKey, cancellationToken).ExpiresUtc).IsEqualTo(DateTimeOffset.FromUnixTimeMilliseconds(cap).UtcDateTime);
        await AssertRepairedRenewalAsync(cluster, cacheKey, operationId, refresh, payload, cap, cancellationToken);

        byte[] replacement = [0, 1];
        _ = await client.SetEntryAsync(Request(key, replacement), cancellationToken: cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, "node-a", WipedCluster.Bound);
        await cluster.StopNodeAsync("node-a");
        var (leaderId, _) = await ledger.LeaderAsync(["node-b", "node-c"], 0UL, "a new leader can replay the access", cancellationToken);
        var newOwner = cluster[leaderId];
        using var replacementChannel = CreateGrpcChannel(newOwner.Uri);
        var replacementClient = new SquirixCacheService.SquirixCacheServiceClient(replacementChannel);
        var replay = await AccessRpcAsync(replacementClient, operationId, key, refresh, cancellationToken);
        _ = await Assert.That(replay != null).IsEqualTo(!refresh);
        if (!refresh)
            _ = await Assert.That(replay.GetValueOrDefault().Span.SequenceEqual(payload)).IsTrue();

        await AssertValueAsync(replacementClient, key, replacement, cancellationToken);
        await AssertReplayAfterRemovalAsync(replacementClient, key, operationId, refresh, payload, cancellationToken);
    }

    /// <summary>Measures the canonical record cost of each sliding operation using equivalent entries.</summary>
    /// <param name="maximum">Whether to exercise the largest legal entry, including the combined Get effect and outcome.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SlidingRefreshOmitsPayloadFromOutcome(bool maximum, CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("renewal-log-size", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var peer = cluster["node-b"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateGrpcChannel(peer.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var log = WipedCluster.Log(owner, "node-a");
        var cap = TimeProvider.System.GetUtcNow().AddMinutes(2).ToUnixTimeMilliseconds();
        var policy = new DistributedCachePolicy(cap, TimeSpan.FromMinutes(5).Ticks);

        // An untagged binary entry adds a 17-byte header and an Int64 deadline.
        var bytes = new byte[maximum ? EntryLimits.MaxEntrySizeBytes - 17 - sizeof(long) - DistributedCachePolicy.HeaderLength : 65536];
        var seed = Request(key, policy.Encode(bytes));
        seed.Entry.ExpiresUtc = Timestamp.FromDateTimeOffset(DateTimeOffset.FromUnixTimeMilliseconds(cap).AddMinutes(-1));
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);
        if (maximum)
        {
            await AssertRenewalRecordLimitAsync(client, log, key, bytes, cancellationToken);
            return;
        }

        var beforeGet = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        var read = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
        var getEntries = await log.GetCommittedEntriesAsync(beforeGet, 2, cancellationToken);
        _ = await Assert.That(read.GetValueOrDefault().Length).IsEqualTo(bytes.Length);
        _ = await Assert.That(getEntries.Count).IsEqualTo(1);

        seed.OperationId = Guid.NewGuid().ToString("N");
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);
        var beforeRefresh = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, true, cancellationToken);
        var refreshEntries = await log.GetCommittedEntriesAsync(beforeRefresh, 2, cancellationToken);
        _ = await Assert.That(refreshEntries.Count).IsEqualTo(1);
        _ = await Assert.That(refreshEntries[0].Payload.Length >= bytes.Length).IsTrue();
        _ = await Assert.That(getEntries[0].Payload.Length - refreshEntries[0].Payload.Length).IsEqualTo(bytes.Length - 3);
    }

    /// <summary>Ordinary reads and Refresh without sliding expiration do not write a group log record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbsoluteAccessDoesNotAppendLog(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("absolute-access", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateGrpcChannel(owner.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            ExpiresAtUnixMs = TimeProvider.System.GetUtcNow().AddMinutes(5).ToUnixTimeMilliseconds(),
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var log = WipedCluster.Log(owner, "node-a");
        var before = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        var read = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
        var refresh = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, true, cancellationToken);
        _ = await Assert.That(read).IsNotNull();
        _ = await Assert.That(read.GetValueOrDefault().Length).IsEqualTo(0);
        _ = await Assert.That(refresh).IsNull();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);
    }

    /// <summary>Reads and refreshes of a missing adapter key leave it absent.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AbsentAccessRemainsMissing(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, cancellationToken: cancellationToken);
        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var read = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), "policy", false, cancellationToken);
        _ = await Assert.That(read).IsNull();
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), "policy", true, cancellationToken);
        var native = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = "policy" }, cancellationToken: cancellationToken);
        _ = await Assert.That(native.Found).IsFalse();
    }

    /// <summary>A journaled Refresh succeeds both when it extends the deadline and when the deadline cannot move.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task JournaledRefreshCanKeepSameDeadline(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = true, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        using var channel = CreateGrpcChannel(node.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "refresh",
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var cache = node.GetRequiredService<ILocalCacheReadOperations<object?>>();
        var key = new CacheKey(CacheName, set.Key);
        var original = clock.GetUtcNow().AddSeconds(10).UtcDateTime;
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        await AssertDistributedEntryAsync(cache, key, set, original, cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        await AssertDistributedEntryAsync(cache, key, set, original.AddSeconds(1), cancellationToken);
    }

    /// <summary>A single node replays the exact renewal response even after a conflicting replacement and journal recovery.</summary>
    /// <param name="persistent">Whether the node uses a journal and can be restarted.</param>
    /// <param name="snapshot">Whether to publish a snapshot of the renewed deadline before restarting.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task SingleNodeAccessReplaysOriginal(bool persistent, bool snapshot, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        if (persistent)
            await node.GetRequiredService<SnapshotTriggerService<object?>>().StopAsync(cancellationToken);

        using var channel = CreateGrpcChannel(node.Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "access",
            Value = ByteString.CopyFrom(255, 0),
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(2));
        var operationId = Guid.NewGuid().ToString("N");
        var access = new GetDistributedAsyncRequest { CacheName = CacheName, Key = request.Key, OperationId = operationId };
        var first = await client.GetDistributedAsync(access, cancellationToken: cancellationToken);
        _ = await Assert.That(first.Found).IsTrue();
        _ = await Assert.That(first.Value).IsEqualTo(request.Value);
        var renewedDeadline = clock.GetUtcNow().AddSeconds(10).UtcDateTime;
        await AssertDistributedEntryAsync(node.GetRequiredService<ILocalCacheReadOperations<object?>>(), new CacheKey(CacheName, request.Key), request, renewedDeadline, cancellationToken);

        if (persistent)
        {
            if (snapshot)
            {
                var coordinator = node.GetRequiredService<Coordinator>();
                _ = await Assert.That(await coordinator.SnapshotAsync(node.GetRequiredService<IJournalCoordinator>(), cancellationToken)).IsTrue();
                var manifest = await node.GetRequiredService<Ledger>().ReadCurrentOrDefaultAsync(cancellationToken);
                _ = await Assert.That(manifest.LastSnapshot).IsNotNull();
            }

            await node.AbruptShutdownAsync();
            await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
            node = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
            await AssertDistributedEntryAsync(node.GetRequiredService<ILocalCacheReadOperations<object?>>(), new CacheKey(CacheName, request.Key), request, renewedDeadline, cancellationToken);
        }

        using var replayChannel = CreateGrpcChannel(node.Uri);
        var replayClient = new SquirixCacheService.SquirixCacheServiceClient(replayChannel);
        byte[] replacement = [1];
        _ = await replayClient.SetEntryAsync(Request(request.Key, replacement), cancellationToken: cancellationToken);
        var replay = await replayClient.GetDistributedAsync(access, cancellationToken: cancellationToken);
        _ = await Assert.That(replay).IsEqualTo(first);
        await AssertValueAsync(replayClient, request.Key, replacement, cancellationToken);
    }

    private static async Task AssertRepairedRenewalAsync(TestCluster<IntegrationStartOptions> cluster, CacheKey key, string operationId, bool refresh, byte[] payload, long cap, CancellationToken cancellationToken)
    {
        var repaired = await cluster.RestartWipedNodeAsync("node-c", null, cancellationToken);
        _ = await WipedCluster.AwaitRepairedCopyAsync(repaired, "node-a", "node-a", cancellationToken);
        ILocalCacheReadOperations<object?> local = repaired.GetRequiredService<PhysicalCache<object?>>();
        var record = local.GetValueWithDeadline(key, cancellationToken);
        _ = await Assert.That(record.ExpiresUtc).IsEqualTo(DateTimeOffset.FromUnixTimeMilliseconds(cap).UtcDateTime);
        var envelope = (await Assert.That(record.Value).IsTypeOf<byte[]>())!;
        _ = await Assert.That(DistributedCacheAccess.ReadPayload(envelope, out _).Span.SequenceEqual(payload)).IsTrue();

        var state = WipedCluster.Log(repaired, "node-a").Idempotency;
        var fingerprint = ReplicaOperationFingerprints.DistributedAccess(operationId, CacheName, key.Key, refresh);
        var lookup = state.Lookup(CacheName, operationId, fingerprint, out var outcome);
        _ = await Assert.That(lookup).IsEqualTo(GroupIdempotencyLookup.Found);
        var reply = ReplicaOutcomeCodec.DecodeAccess(outcome.OutcomePayload);
        _ = await Assert.That(reply != null).IsEqualTo(!refresh);
        if (!refresh)
            _ = await Assert.That(reply.GetValueOrDefault().Span.SequenceEqual(payload)).IsTrue();
    }

    private static async Task AssertRenewalRecordLimitAsync(SquirixCacheService.SquirixCacheServiceClient client, IFollowerLog log, string key, byte[] bytes, CancellationToken cancellationToken)
    {
        var before = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        var pending = AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
        var getFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(pending, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(getFailure.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);

        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            Value = ByteString.CopyFrom(bytes),
            SlidingTicks = TimeSpan.FromMinutes(5).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var write = client.SetDistributedAsync(set, cancellationToken: cancellationToken).ResponseAsync;
        var setFailure = await NodeAsyncAssert.ThrowsAsync<RpcException>(write, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(setFailure.StatusCode).IsEqualTo(StatusCode.ResourceExhausted);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);

        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, true, cancellationToken);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before + 1);
    }

    private static async Task AssertReplayAfterRemovalAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, string operationId, bool refresh, byte[] payload, CancellationToken cancellationToken)
    {
        _ = await client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") }, cancellationToken: cancellationToken);
        var afterRemoval = await AccessRpcAsync(client, operationId, key, refresh, cancellationToken);
        _ = await Assert.That(afterRemoval != null).IsEqualTo(!refresh);
        if (!refresh)
            _ = await Assert.That(afterRemoval.GetValueOrDefault().Span.SequenceEqual(payload)).IsTrue();

        var wrongOperation = AccessRpcAsync(client, operationId, key, !refresh, cancellationToken);
        var mismatch = await NodeAsyncAssert.ThrowsAsync<RpcException>(wrongOperation, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(mismatch.StatusCode).IsEqualTo(StatusCode.FailedPrecondition);
        var missing = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
        _ = await Assert.That(missing).IsNull();
    }

    private static async Task AssertPolicyReplacementAsync(SquirixCacheService.SquirixCacheServiceClient client, SetDistributedAsyncRequest request, FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        var elapsed = request.Clone();
        elapsed.OperationId = Guid.NewGuid().ToString("N");
        elapsed.ExpiresAtUnixMs = clock.GetUtcNow().AddMilliseconds(-1).ToUnixTimeMilliseconds();
        _ = await client.SetDistributedAsync(elapsed, cancellationToken: cancellationToken);
        var readRequest = new GetEntryAsyncRequest { CacheName = CacheName, Key = request.Key };
        _ = await Assert.That((await client.GetEntryAsync(readRequest, cancellationToken: cancellationToken)).Found).IsFalse();

        var permanent = request.Clone();
        permanent.OperationId = Guid.NewGuid().ToString("N");
        permanent.ClearExpiresAtUnixMs();
        permanent.ClearTtlTicks();
        permanent.ClearSlidingTicks();
        _ = await client.SetDistributedAsync(permanent, cancellationToken: cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(1));
        var read = await client.GetEntryAsync(readRequest, cancellationToken: cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Entry.ExpiresUtc).IsNull();
    }

    private static async Task AssertInvalidPolicyRejectedAsync(SquirixCacheService.SquirixCacheServiceClient client, SetDistributedAsyncRequest request, CancellationToken cancellationToken)
    {
        var invalid = request.Clone();
        invalid.OperationId = Guid.NewGuid().ToString("N");
        invalid.SlidingTicks = 0;
        var rejection = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.SetDistributedAsync(invalid, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejection.StatusCode).IsEqualTo(StatusCode.InvalidArgument);
    }

    private static async Task<ReadOnlyMemory<byte>?> AccessRpcAsync(SquirixCacheService.SquirixCacheServiceClient client, string operationId, string key, bool refresh, CancellationToken cancellationToken)
    {
        if (refresh)
        {
            _ = await client.RefreshDistributedAsync(new RefreshDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken);
            return null;
        }

        var response = await client.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken);
        return response.Found ? response.Value.Memory : default(ReadOnlyMemory<byte>?);
    }

    private static async Task AssertDistributedEntryAsync(ILocalCacheReadOperations<object?> cache, CacheKey key, SetDistributedAsyncRequest request, DateTime expectedDeadline, CancellationToken cancellationToken)
    {
        var read = cache.GetValueWithDeadline(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.ExpiresUtc).IsEqualTo(expectedDeadline);
        var stored = (await Assert.That(read.Value).IsTypeOf<byte[]>())!;
        _ = await Assert.That(DistributedCachePolicy.TryRead(stored, out var policy)).IsTrue();
        long? cap = request.HasExpiresAtUnixMs ? request.ExpiresAtUnixMs : null;
        long? sliding = request.HasSlidingTicks ? request.SlidingTicks : null;
        _ = await Assert.That(policy.AbsoluteUnixMilliseconds).IsEqualTo(cap);
        _ = await Assert.That(policy.SlidingTicks).IsEqualTo(sliding);
        _ = await Assert.That(stored.AsSpan(DistributedCachePolicy.HeaderLength).SequenceEqual(request.Value.Span)).IsTrue();
    }

    private static SetEntryAsyncRequest Request(string key, byte[] value) => new()
    {
        CacheName = CacheName,
        Key = key,
        OperationId = Guid.NewGuid().ToString("N"),
        Entry = new CacheEntryWire { Value = new CacheValue { BinaryValue = ByteString.CopyFrom(value) } },
    };

    private static async Task AssertValueAsync(SquirixCacheService.SquirixCacheServiceClient client, string key, byte[] expected, CancellationToken cancellationToken)
    {
        var result = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(result.Value.BinaryValue.Span.SequenceEqual(expected)).IsTrue();
    }
}
