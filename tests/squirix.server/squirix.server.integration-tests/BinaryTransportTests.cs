using System;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Replication;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;
using Squirix.Testing;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests;

/// <summary>Exercises raw binary transport through durability and request validation.</summary>
public sealed class BinaryTransportTests : NodeIntegrationTestBase
{
    private const string NodeId = "binary-node";
    private const string CacheName = "binary";

    /// <summary>Read and mutation handlers retain their respective error order for an invalid cache name and key.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidKeyKeepsWireErrorOrder(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions(), cancellationToken);
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri);
        var client = new CacheWireClient(channel);
        var timeout = TimeSpan.FromSeconds(30);

        var read = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.GetEntryAsync(new GetEntryAsyncRequest(), cancellationToken: cancellationToken), timeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(read.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(read.Message).IsEqualTo("Cache key is required.");

        var remove = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.RemoveAsync(new RemoveAsyncRequest(), cancellationToken: cancellationToken), timeout, TimeProvider.System, cancellationToken);
        _ = await Assert.That(remove.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(remove.Message).IsEqualTo("cache_name is required for internal cluster RPCs.");

        var routed = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = "bad\u0001" }, cancellationToken: cancellationToken),
            timeout,
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(routed.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
        _ = await Assert.That(routed.Message).IsEqualTo("Cache key contains control characters.");
    }

    /// <summary>Large removals keep only booleans and replay without deleting a replacement.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LargeRemoveKeepsBudgetAndReplay(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("compact-remove", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateBinaryChannel(cluster["node-b"].Uri);
        var client = new CacheWireClient(channel);
        var log = WipedCluster.Log(owner, "node-a");
        var value = new byte[4190208];
        var first = new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") };
        for (var i = 0; i < 32; i++)
        {
            _ = await client.SetEntryAsync(Request(key, value), cancellationToken: cancellationToken);
            var remove = i == 0 ? first : new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") };
            _ = await Assert.That((await client.RemoveAsync(remove, cancellationToken: cancellationToken)).Removed).IsTrue();
        }

        _ = await Assert.That(log.Idempotency.OutcomeBytes < 65536L).IsTrue();
        foreach (var outcome in log.Idempotency.ExportResolved(out _))
            _ = await Assert.That(outcome.OutcomePayload.Length).IsEqualTo(5);
        var missing = new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") };
        _ = await Assert.That((await client.RemoveAsync(missing, cancellationToken: cancellationToken)).Removed).IsFalse();
        _ = await client.SetEntryAsync(Request(key, [7]), cancellationToken: cancellationToken);
        _ = await Assert.That((await client.RemoveAsync(first, cancellationToken: cancellationToken)).Removed).IsTrue();
        _ = await Assert.That((await client.RemoveAsync(missing, cancellationToken: cancellationToken)).Removed).IsFalse();
        await AssertValueAsync(client, key, [7], cancellationToken);
    }

    /// <summary>Reads never compute mutation fingerprints or retain response replay state.</summary>
    /// <param name="persistent">Whether outcomes are journaled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DistributedAccessReadsCurrentEntry(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        using var channel = CreateBinaryChannel(node.Uri);
        var client = new CacheWireClient(channel);
        var access = node.GetRequiredService<IDistributedCacheAccess>();
        var missing = await access.AccessAsync(string.Empty, CacheName, "lazy", false, cancellationToken);
        _ = await Assert.That(missing).IsNull();
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "lazy",
            Value = new byte[] { 3, 5, 8 },
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var unchanged = await access.AccessAsync(string.Empty, CacheName, "lazy", false, cancellationToken);
        _ = await Assert.That(unchanged!.Value.Span.SequenceEqual(new byte[] { 3, 5, 8 })).IsTrue();
        _ = await access.AccessAsync(Guid.NewGuid().ToString("N"), CacheName, "lazy", true, cancellationToken);

        clock.Advance(TimeSpan.FromMilliseconds(1));
        var operationId = Guid.NewGuid().ToString("N");
        var renewed = await access.AccessAsync(string.Empty, CacheName, "lazy", false, cancellationToken);
        _ = await Assert.That(renewed!.Value.Span.SequenceEqual(new byte[] { 3, 5, 8 })).IsTrue();
        set.OperationId = Guid.NewGuid().ToString("N");
        set.Value = new byte[] { 13 };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var replay = await access.AccessAsync(string.Empty, CacheName, "lazy", false, cancellationToken);
        _ = await Assert.That(replay!.Value.Span.SequenceEqual(new byte[] { 13 })).IsTrue();
        var refreshed = await access.AccessAsync(operationId, CacheName, "lazy", true, cancellationToken);
        _ = await Assert.That(refreshed).IsNull();

        var read = new GetDistributedAsyncRequest { CacheName = CacheName, Key = "empty" };
        var miss = await client.GetDistributedAsync(read, cancellationToken: cancellationToken);
        _ = await Assert.That(miss.Found).IsFalse();
        set.Key = read.Key;
        set.OperationId = Guid.NewGuid().ToString("N");
        set.Value = ReadOnlyMemory<byte>.Empty;
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var hit = await client.GetDistributedAsync(read, cancellationToken: cancellationToken);
        _ = await Assert.That(hit.Found).IsTrue();
        _ = await Assert.That(hit.Value.IsEmpty).IsTrue();
    }

    /// <summary>Concurrent calls through a shared gRPC service keep cache names and payloads isolated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentNamespacesStayIsolated(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, cancellationToken: cancellationToken);
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri);
        var client = new CacheWireClient(channel);
        var writes = new Task[32];
        for (var i = 0; i < writes.Length; i++)
        {
            byte[] bytes = [byte.CreateChecked(i), 255];
            writes[i] = client.SetDistributedAsync(
                new SetDistributedAsyncRequest
                {
                    CacheName = string.Create(CultureInfo.InvariantCulture, $"{CacheName}{i}"),
                    Key = "shared-key",
                    Value = bytes,
                    OperationId = Guid.NewGuid().ToString("N"),
                },
                cancellationToken: cancellationToken);
        }

        await Task.WhenAll(writes);
        var reads = new Task<GetDistributedAsyncResponse>[writes.Length];
        for (var i = 0; i < reads.Length; i++)
        {
            reads[i] = client.GetDistributedAsync(
                new GetDistributedAsyncRequest
                {
                    CacheName = string.Create(CultureInfo.InvariantCulture, $"{CacheName}{i}"),
                    Key = "shared-key",
                },
                cancellationToken: cancellationToken);
        }

        var responses = await Task.WhenAll(reads);
        for (var i = 0; i < responses.Length; i++)
        {
            _ = await Assert.That(responses[i].Found).IsTrue();
            _ = await Assert.That(responses[i].Value.Span.SequenceEqual<byte>([byte.CreateChecked(i), 255])).IsTrue();
        }
    }

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
        await node.GetRequiredService<SnapshotTriggerService>().StopAsync(cancellationToken);
        byte[] expected = [0, 255, 128, 34, 0];
        using (var channel = CreateBinaryChannel(node.Uri))
        {
            var client = new CacheWireClient(channel);
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
            if (manifest.LastSnapshot is { } published)
            {
                var loaded = await node.GetRequiredService<ISnapshotReader>().LoadStrictAsync<StoredValue>(SnapshotFileName.BuildPath(node.DataDir, published.Index), null, cancellationToken);
                _ = await Assert.That(loaded.Entries.Count).IsEqualTo(1);
                var bytes = StoredValueProbe.Binary(loaded.Entries[0].Entry.Value);
                _ = await Assert.That(bytes.AsSpan().SequenceEqual(expected)).IsTrue();
            }
        }

        await node.AbruptShutdownAsync();
        await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
        var restarted = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
        using var recoveredChannel = CreateBinaryChannel(restarted.Uri);
        var recovered = new CacheWireClient(recoveredChannel);
        await AssertValueAsync(recovered, "snapshot", expected, cancellationToken);
        await AssertValueAsync(recovered, "tail", expected, cancellationToken);
        await AssertValueAsync(recovered, "empty", [], cancellationToken);
    }

    /// <summary>The replicated value limit accepts the exact binary boundary and rejects one extra byte without overwriting.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BinarySizeBoundaryIsAtomic(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("binary-size-boundary", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateBinaryChannel(owner.Uri);
        var client = new CacheWireClient(channel);

        var maximum = new byte[EntryLimits.MaxReplicatedValueBytes];
        maximum[^1] = 255;
        _ = await client.SetEntryAsync(Request(key, maximum), cancellationToken: cancellationToken);
        var tooLarge = new byte[maximum.Length + 1];
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.SetEntryAsync(Request(key, tooLarge), cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.ResourceExhausted);
        _ = await Assert.That(failure.FindTrailer(Frames.ErrorCodeTrailer)).IsEqualTo(Frames.PayloadTooLargeCode);
        await AssertValueAsync(client, key, maximum, cancellationToken);
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
        using var channel = CreateBinaryChannel(cluster["node-b"].Uri);
        var client = new CacheWireClient(channel);
        _ = await client.SetEntryAsync(Request(key, expected), cancellationToken: cancellationToken);
        foreach (var id in WipedCluster.Nodes)
        {
            var local = cluster[id].GetRequiredService<PhysicalCache<StoredValue>>();
            await (Cache: local, Key: new CacheKey(CacheName, key), Expected: expected).WaitUntilValueAsync(
                static (state, token) => ValueTask.FromResult(state.Cache.GetValue(state.Key, token).Value is { Kind: StoredValue.ValueKind.Binary } binary && binary.Bytes.AsSpan().SequenceEqual(state.Expected)),
                WipedCluster.Bound,
                cancellationToken);
        }

        var repaired = await cluster.RestartWipedNodeAsync("node-c", cancellationToken);
        _ = await WipedCluster.AwaitRepairedCopyAsync(repaired, "node-a", "node-a", cancellationToken);
        var recovered = repaired.GetRequiredService<PhysicalCache<StoredValue>>().GetValue(new CacheKey(CacheName, key), cancellationToken);
        var value = StoredValueProbe.Binary(recovered.Value);
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
    public async Task DistributedSetRecomputesDeadline(bool persistent, bool relative, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1).AddTicks(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        if (persistent)
            await node.GetRequiredService<SnapshotTriggerService>().StopAsync(cancellationToken);

        var key = new CacheKey(CacheName, "policy");
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri);
        var client = new CacheWireClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "policy",
            Value = new byte[] { 255, 0, 128 },
            SlidingTicks = TimeSpan.FromSeconds(20).Ticks + 1,
            ExpiresAtUnixMs = clock.GetUtcNow().AddSeconds(30).ToUnixTimeMilliseconds(),
            OperationId = Guid.NewGuid().ToString("N"),
        };
        var expectedPolicy = SetDistributedAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        var initial = DateTime.UnixEpoch.AddDays(1).AddSeconds(relative ? 10 : 20);
        if (relative)
        {
            request.TtlTicks = TimeSpan.FromSeconds(10).Ticks;
            expectedPolicy.ExpiresAtUnixMs = new DateTimeOffset(initial).ToUnixTimeMilliseconds();
        }

        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), key, expectedPolicy, initial, cancellationToken);

        clock.Advance(TimeSpan.FromSeconds(2));
        if (persistent)
        {
            await node.AbruptShutdownAsync();
            await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
            _ = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
        }

        using var replayChannel = CreateBinaryChannel(cluster[NodeId].Uri);
        var replay = new CacheWireClient(replayChannel);
        _ = await replay.SetDistributedAsync(request, cancellationToken: cancellationToken);
        var nextDeadline = initial.AddSeconds(2);
        if (relative)
            expectedPolicy.ExpiresAtUnixMs = new DateTimeOffset(nextDeadline).ToUnixTimeMilliseconds();

        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), key, expectedPolicy, nextDeadline, cancellationToken);
        await AssertInvalidPolicyRejectedAsync(replay, request, cancellationToken);
        await AssertDistributedEntryAsync(cluster[NodeId].GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), key, expectedPolicy, nextDeadline, cancellationToken);
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
        using var channel = CreateBinaryChannel(cluster["node-b"].Uri);
        var client = new CacheWireClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = key,
            Value = new byte[] { 0, 255 },
            SlidingTicks = TimeSpan.FromMinutes(5).Ticks,
            ExpiresAtUnixMs = TimeProvider.System.GetUtcNow().AddMinutes(10).ToUnixTimeMilliseconds(),
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        var cacheKey = new CacheKey(CacheName, key);
        var leader = cluster["node-a"].GetRequiredService<ILocalCacheReadOperations<StoredValue>>();
        var deadline = leader.GetValueWithDeadline(cacheKey, cancellationToken).ExpiresUtc;
        _ = await Assert.That(deadline).IsNotNull();
        foreach (var id in WipedCluster.Nodes)
        {
            var local = cluster[id].GetRequiredService<ILocalCacheReadOperations<StoredValue>>();
            await (Cache: local, Key: cacheKey).WaitUntilValueAsync(
                static (state, token) => ValueTask.FromResult(state.Cache.GetValueWithDeadline(state.Key, token).Found),
                WipedCluster.Bound,
                cancellationToken);
            await AssertDistributedEntryAsync(local, cacheKey, request, deadline.Value, cancellationToken);
        }

        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        await AssertDistributedEntryAsync(leader, cacheKey, request, deadline.Value, cancellationToken);
        var changed = SetDistributedAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        changed.ExpiresAtUnixMs++;
        var conflict = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.SetDistributedAsync(changed, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(conflict.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
    }

    /// <summary>A new leader reads the replacement on a repeated Get while Refresh preserves mutation replay.</summary>
    /// <param name="refresh">Whether to exercise the payload-free refresh outcome.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DistributedAccessRetriesReadCurrent(bool refresh, CancellationToken cancellationToken)
    {
        var loss = new LostForwardResponseProbe();
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("distributed-access", true, services: loss.Register), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateBinaryChannel(owner.Uri);
        var client = new CacheWireClient(channel);
        var cap = TimeProvider.System.GetUtcNow().AddMinutes(2).ToUnixTimeMilliseconds();
        var policy = new DistributedCachePolicy(cap, TimeSpan.FromMinutes(5).Ticks);
        byte[] payload = [255, 0, 128];
        var seed = Request(key, policy.Encode(payload));
        seed.Entry!.ExpiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(cap).AddMinutes(-1).UtcDateTime;
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);

        var operationId = Guid.NewGuid().ToString("N");
        loss.Lose(key, static () => Task.CompletedTask);
        using var entryChannel = CreateBinaryChannel(cluster["node-b"].Uri);
        var entryClient = new CacheWireClient(entryChannel);
        var first = AccessRpcAsync(entryClient, operationId, key, refresh, cancellationToken);
        var lost = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(first, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(lost.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(loss.OwnerExecutions(key)).IsEqualTo(1);

        var stored = owner.GetRequiredService<ILocalCacheReadOperations<StoredValue>>();
        var cacheKey = new CacheKey(CacheName, key);
        _ = await Assert.That(stored.GetValueWithDeadline(cacheKey, cancellationToken).ExpiresUtc).IsEqualTo(DateTimeOffset.FromUnixTimeMilliseconds(cap).UtcDateTime);
        await AssertRepairedRenewalAsync(cluster, cacheKey, operationId, refresh, payload, cap, cancellationToken);

        byte[] replacement = [0, 1];
        _ = await client.SetEntryAsync(Request(key, new DistributedCachePolicy(null, null).Encode(replacement)), cancellationToken: cancellationToken);
        var ledger = new GroupAuthorityLedger<IntegrationStartOptions>(cluster, "node-a", WipedCluster.Bound);
        await cluster.StopNodeAsync("node-a");
        var (leaderId, _) = await ledger.LeaderAsync(["node-b", "node-c"], 0UL, "a new leader can replay the access", cancellationToken);
        var newOwner = cluster[leaderId];
        using var replacementChannel = CreateBinaryChannel(newOwner.Uri);
        var replacementClient = new CacheWireClient(replacementChannel);
        var replay = await AccessRpcAsync(replacementClient, operationId, key, refresh, cancellationToken);
        _ = await Assert.That(replay != null).IsEqualTo(!refresh);
        if (!refresh)
            _ = await Assert.That(replay.GetValueOrDefault().Span.SequenceEqual(replacement)).IsTrue();

        await AssertValueAsync(replacementClient, key, new DistributedCachePolicy(null, null).Encode(replacement), cancellationToken);
        await AssertReplayAfterRemovalAsync(replacementClient, key, operationId, refresh, cancellationToken);
    }

    /// <summary>Measures the canonical record cost of each sliding operation using equivalent entries.</summary>
    /// <param name="maximum">Whether to exercise the largest legal entry without duplicating its payload in the outcome.</param>
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
        using var channel = CreateBinaryChannel(peer.Uri);
        var client = new CacheWireClient(channel);
        var log = WipedCluster.Log(owner, "node-a");
        var cap = TimeProvider.System.GetUtcNow().AddMinutes(2).ToUnixTimeMilliseconds();
        var policy = new DistributedCachePolicy(cap, TimeSpan.FromMinutes(5).Ticks);

        // An untagged binary entry adds a 17-byte header and an Int64 deadline.
        var bytes = new byte[maximum ? EntryLimitKit.LargeFixtureBytes - 17 - sizeof(long) - DistributedCachePolicy.HeaderLength : 65536];
        _ = await client.SetDistributedAsync(
            new SetDistributedAsyncRequest
            {
                CacheName = CacheName,
                Key = key,
                Value = bytes,
                SlidingTicks = TimeSpan.FromMinutes(5).Ticks,
                ExpiresAtUnixMs = cap,
                OperationId = Guid.NewGuid().ToString("N"),
            },
            cancellationToken: cancellationToken);
        var seed = Request(key, policy.Encode(bytes));
        seed.Entry!.ExpiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(cap).AddMinutes(-1).UtcDateTime;
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);
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
        _ = await Assert.That(getEntries[0].Payload.Length - refreshEntries[0].Payload.Length).IsEqualTo(-4);
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
        using var channel = CreateBinaryChannel(owner.Uri);
        var client = new CacheWireClient(channel);
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
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri);
        var client = new CacheWireClient(channel);
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
        using var channel = CreateBinaryChannel(node.Uri);
        var client = new CacheWireClient(channel);
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "refresh",
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var cache = node.GetRequiredService<ILocalCacheReadOperations<StoredValue>>();
        var key = new CacheKey(CacheName, set.Key);
        var original = clock.GetUtcNow().AddSeconds(10).UtcDateTime;
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        await AssertDistributedEntryAsync(cache, key, set, original, cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        await AssertDistributedEntryAsync(cache, key, set, original.AddSeconds(1), cancellationToken);
    }

    /// <summary>Accesses that cannot move a millisecond deadline do not retain mutation outcomes.</summary>
    /// <param name="persistent">Whether the node journals mutations.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task UnchangedDeadlineSkipsRenewal(bool persistent, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        using var channel = CreateBinaryChannel(node.Uri);
        var client = new CacheWireClient(channel);
        var set = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "unchanged-renewal",
            Value = new byte[] { 255, 0 },
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(set, cancellationToken: cancellationToken);
        var deadline = clock.GetUtcNow().AddSeconds(10).UtcDateTime;
        clock.Advance(TimeSpan.FromTicks(TimeSpan.TicksPerMillisecond - 1));
        for (var i = 0; i < 100; i++)
        {
            var value = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, false, cancellationToken);
            _ = await Assert.That(value.GetValueOrDefault().Span.SequenceEqual(set.Value.Span)).IsTrue();
            _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        }

        clock.Advance(TimeSpan.FromTicks(1));
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, false, cancellationToken);
        _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), set.Key, true, cancellationToken);
        await AssertDistributedEntryAsync(node.GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), new CacheKey(CacheName, set.Key), set, deadline.AddMilliseconds(1), cancellationToken);
    }

    /// <summary>A capped sliding entry is a fenced read, not a new replicated outcome on every access.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CappedSlidingAccessKeepsLogIndex(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("node-a", "node-b", "node-c", WipedCluster.Options("renewal-at-cap", true), cancellationToken);
        await WipedCluster.AwaitOwnerLeadsAsync(cluster, cancellationToken);
        var owner = cluster["node-a"];
        var key = owner.FindKeyOwnedBy(CacheName, "node-a");
        using var channel = CreateBinaryChannel(owner.Uri);
        var client = new CacheWireClient(channel);
        var cap = TimeProvider.System.GetUtcNow().AddMinutes(5).ToUnixTimeMilliseconds();
        var policy = new DistributedCachePolicy(cap, TimeSpan.FromMinutes(10).Ticks);
        var seed = Request(key, policy.Encode([1, 255]));
        seed.Entry!.ExpiresUtc = DateTimeOffset.FromUnixTimeMilliseconds(cap).UtcDateTime;
        _ = await client.SetEntryAsync(seed, cancellationToken: cancellationToken);
        var log = WipedCluster.Log(owner, "node-a");
        var before = (await log.GetStatusAsync(cancellationToken)).LastLogIndex;
        for (var i = 0; i < 8; i++)
        {
            var value = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
            _ = await Assert.That(value.GetValueOrDefault().Span.SequenceEqual(new byte[] { 1, 255 })).IsTrue();
            _ = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, true, cancellationToken);
        }

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(before);
    }

    /// <summary>A repeated Get reads the current entry after journal recovery or snapshot recovery.</summary>
    /// <param name="persistent">Whether the node uses a journal and can be restarted.</param>
    /// <param name="snapshot">Whether to publish a snapshot of the renewed deadline before restarting.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(true, true)]
    public async Task SingleNodeAccessReadsCurrent(bool persistent, bool snapshot, CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch.AddDays(1));
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions { UsePersistence = persistent, TimeProvider = clock }, cancellationToken);
        var node = cluster[NodeId];
        if (persistent)
            await node.GetRequiredService<SnapshotTriggerService>().StopAsync(cancellationToken);

        using var channel = CreateBinaryChannel(node.Uri);
        var client = new CacheWireClient(channel);
        var request = new SetDistributedAsyncRequest
        {
            CacheName = CacheName,
            Key = "access",
            Value = new byte[] { 255, 0 },
            SlidingTicks = TimeSpan.FromSeconds(10).Ticks,
            OperationId = Guid.NewGuid().ToString("N"),
        };
        _ = await client.SetDistributedAsync(request, cancellationToken: cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(2));
        var access = new GetDistributedAsyncRequest { CacheName = CacheName, Key = request.Key };
        var first = await client.GetDistributedAsync(access, cancellationToken: cancellationToken);
        var missingRemove = new RemoveAsyncRequest { CacheName = CacheName, Key = "missing-replay", OperationId = Guid.NewGuid().ToString("N") };
        _ = await Assert.That((await client.RemoveAsync(missingRemove, cancellationToken: cancellationToken)).Removed).IsFalse();
        _ = await Assert.That(first.Found).IsTrue();
        _ = await Assert.That(first.Value.Span.SequenceEqual(request.Value.Span)).IsTrue();
        var renewedDeadline = clock.GetUtcNow().AddSeconds(10).UtcDateTime;
        await AssertDistributedEntryAsync(node.GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), new CacheKey(CacheName, request.Key), request, renewedDeadline, cancellationToken);

        if (persistent)
        {
            if (snapshot)
                await AssertRenewalSnapshotAsync(node, request.Key, clock.GetUtcNow().UtcDateTime, renewedDeadline, cancellationToken);

            await node.AbruptShutdownAsync();
            await JournalSegmentLeaseWait.WaitForReleasedAsync(node.DataDir, cancellationToken);
            node = await cluster.RestartNodeAsync(NodeId, null, cancellationToken);
            await AssertDistributedEntryAsync(node.GetRequiredService<ILocalCacheReadOperations<StoredValue>>(), new CacheKey(CacheName, request.Key), request, renewedDeadline, cancellationToken);
        }

        using var replayChannel = CreateBinaryChannel(node.Uri);
        var replayClient = new CacheWireClient(replayChannel);
        byte[] replacement = [1];
        _ = await replayClient.SetEntryAsync(Request(request.Key, new DistributedCachePolicy(null, null).Encode(replacement)), cancellationToken: cancellationToken);
        var replay = await replayClient.GetDistributedAsync(access, cancellationToken: cancellationToken);
        _ = await Assert.That(replay.Value.Span.SequenceEqual(replacement)).IsTrue();
        await AssertValueAsync(replayClient, request.Key, new DistributedCachePolicy(null, null).Encode(replacement), cancellationToken);
        _ = await replayClient.SetEntryAsync(Request(missingRemove.Key, [7]), cancellationToken: cancellationToken);
        _ = await Assert.That((await replayClient.RemoveAsync(missingRemove, cancellationToken: cancellationToken)).Removed).IsTrue();
        _ = await Assert.That((await replayClient.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = missingRemove.Key }, cancellationToken: cancellationToken)).Found).IsFalse();
    }

    /// <summary>Single-copy writes do not require an operation ID and each execution sees current state.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SingleCopyWriteAcceptsMissingId(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, new IntegrationStartOptions(), cancellationToken);
        using var channel = CreateBinaryChannel(cluster[NodeId].Uri);
        var client = new CacheWireClient(channel);
        var add = new TryAddEntryAsyncRequest
        {
            CacheName = CacheName,
            Key = "ordinary-add",
            Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "value" } },
        };

        _ = await Assert.That((await client.AddEntryIfAbsentAsync(add, cancellationToken: cancellationToken)).Added).IsTrue();
        _ = await Assert.That((await client.AddEntryIfAbsentAsync(add, cancellationToken: cancellationToken)).Added).IsFalse();
    }

    private static async Task AssertRenewalSnapshotAsync(ITestNodeHost node, string key, DateTime cutoff, DateTime deadline, CancellationToken cancellationToken)
    {
        var coordinator = node.GetRequiredService<Coordinator>();
        _ = await Assert.That(await coordinator.SnapshotAsync(node.GetRequiredService<IJournalCoordinator>(), cancellationToken)).IsTrue();
        var manifest = await node.GetRequiredService<Ledger>().ReadCurrentOrDefaultAsync(cancellationToken);
        _ = await Assert.That(manifest.LastSnapshot).IsNotNull();
        var path = SnapshotFileName.BuildPath(node.DataDir, manifest.LastSnapshot.Index);
        var loaded = await node.GetRequiredService<ISnapshotReader>().LoadStrictAsync<StoredValue>(path, cutoff, cancellationToken);
        _ = await Assert.That(loaded.Entries.Count).IsEqualTo(1);
        _ = await Assert.That(loaded.Entries[0].Key).IsEqualTo(new CacheKey(CacheName, key));
        _ = await Assert.That(loaded.Entries[0].Entry.ExpiresUtc).IsEqualTo(deadline);
    }

    private static async Task AssertRepairedRenewalAsync(TestCluster<IntegrationStartOptions> cluster, CacheKey key, string operationId, bool refresh, byte[] payload, long cap, CancellationToken cancellationToken)
    {
        var repaired = await cluster.RestartWipedNodeAsync("node-c", cancellationToken);
        _ = await WipedCluster.AwaitRepairedCopyAsync(repaired, "node-a", "node-a", cancellationToken);
        ILocalCacheReadOperations<StoredValue> local = repaired.GetRequiredService<PhysicalCache<StoredValue>>();
        var record = local.GetValueWithDeadline(key, cancellationToken);
        _ = await Assert.That(record.ExpiresUtc).IsEqualTo(DateTimeOffset.FromUnixTimeMilliseconds(cap).UtcDateTime);
        var envelope = StoredValueProbe.Binary(record.Value);
        _ = await Assert.That(DistributedCacheAccess.ReadPayload(envelope, out _).Span.SequenceEqual(payload)).IsTrue();

        var state = WipedCluster.Log(repaired, "node-a").Idempotency;
        var fingerprint = ReplicaOperationFingerprints.DistributedAccess(operationId, CacheName, key.Key, refresh);
        var lookup = state.Lookup(CacheName, operationId, fingerprint, out var outcome);
        _ = await Assert.That(lookup).IsEqualTo(refresh ? GroupIdempotencyLookup.Found : GroupIdempotencyLookup.Miss);
        if (refresh)
            _ = await Assert.That(ReplicaOutcomeCodec.DecodeApplied(outcome.OutcomePayload)).IsTrue();
        foreach (var result in state.ExportResolved(out _))
            _ = await Assert.That(result.OutcomePayload.Length).IsEqualTo(5);
    }

    private static async Task AssertReplayAfterRemovalAsync(CacheWireClient client, string key, string operationId, bool refresh, CancellationToken cancellationToken)
    {
        _ = await client.RemoveAsync(new RemoveAsyncRequest { CacheName = CacheName, Key = key, OperationId = Guid.NewGuid().ToString("N") }, cancellationToken: cancellationToken);
        var afterRemoval = await AccessRpcAsync(client, operationId, key, refresh, cancellationToken);
        _ = await Assert.That(afterRemoval).IsNull();
        _ = await Assert.That(await AccessRpcAsync(client, operationId, key, !refresh, cancellationToken)).IsNull();
        var missing = await AccessRpcAsync(client, Guid.NewGuid().ToString("N"), key, false, cancellationToken);
        _ = await Assert.That(missing).IsNull();
    }

    private static async Task AssertPolicyReplacementAsync(CacheWireClient client, SetDistributedAsyncRequest request, FakeTimeProvider clock, CancellationToken cancellationToken)
    {
        var elapsed = SetDistributedAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        elapsed.OperationId = Guid.NewGuid().ToString("N");
        elapsed.ExpiresAtUnixMs = clock.GetUtcNow().AddMilliseconds(-1).ToUnixTimeMilliseconds();
        _ = await client.SetDistributedAsync(elapsed, cancellationToken: cancellationToken);
        var readRequest = new GetEntryAsyncRequest { CacheName = CacheName, Key = request.Key };
        _ = await Assert.That((await client.GetEntryAsync(readRequest, cancellationToken: cancellationToken)).Found).IsFalse();

        var permanent = new SetDistributedAsyncRequest { CacheName = request.CacheName, Key = request.Key, Value = request.Value, OperationId = Guid.NewGuid().ToString("N") };
        _ = await client.SetDistributedAsync(permanent, cancellationToken: cancellationToken);
        clock.Advance(TimeSpan.FromMinutes(1));
        var read = await client.GetEntryAsync(readRequest, cancellationToken: cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Entry!.ExpiresUtc).IsNull();
    }

    private static async Task AssertInvalidPolicyRejectedAsync(CacheWireClient client, SetDistributedAsyncRequest request, CancellationToken cancellationToken)
    {
        var invalid = SetDistributedAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        invalid.OperationId = Guid.NewGuid().ToString("N");
        invalid.SlidingTicks = 0;
        var rejection = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.SetDistributedAsync(invalid, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(rejection.StatusCode).IsEqualTo(FrameStatus.InvalidArgument);
    }

    private static async Task<ReadOnlyMemory<byte>?> AccessRpcAsync(CacheWireClient client, string operationId, string key, bool refresh, CancellationToken cancellationToken)
    {
        if (refresh)
        {
            _ = await client.RefreshDistributedAsync(new RefreshDistributedAsyncRequest { CacheName = CacheName, Key = key, OperationId = operationId }, cancellationToken: cancellationToken);
            return null;
        }

        var response = await client.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        return response.Found ? response.Value : default(ReadOnlyMemory<byte>?);
    }

    private static async Task AssertDistributedEntryAsync(ILocalCacheReadOperations<StoredValue> cache, CacheKey key, SetDistributedAsyncRequest request, DateTime expectedDeadline, CancellationToken cancellationToken)
    {
        var read = cache.GetValueWithDeadline(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.ExpiresUtc).IsEqualTo(expectedDeadline);
        var stored = StoredValueProbe.Binary(read.Value);
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
        Entry = new CacheEntryWire { Value = new CacheValue { BinaryValue = value } },
    };

    private static async Task AssertValueAsync(CacheWireClient client, string key, byte[] expected, CancellationToken cancellationToken)
    {
        var result = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = key }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value!.KindCase).IsEqualTo(CacheValue.KindOneofCase.BinaryValue);
        _ = await Assert.That(result.Value.BinaryValue.Span.SequenceEqual(expected)).IsTrue();
    }
}
