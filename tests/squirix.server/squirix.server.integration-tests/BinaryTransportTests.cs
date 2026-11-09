using System;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Grpc.Core;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
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

    /// <summary>Staged distributed policy operations fail explicitly and cannot silently store an ignored policy.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnsupportedPolicyIsNotApplied(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(NodeId, cancellationToken: cancellationToken);
        using var channel = CreateGrpcChannel(cluster[NodeId].Uri);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var setting = client.SetDistributedAsync(
            new SetDistributedAsyncRequest { CacheName = CacheName, Key = "policy", Value = ByteString.CopyFrom(255), SlidingExpirationTicks = 1, OperationId = Guid.NewGuid().ToString("N") },
            cancellationToken: cancellationToken).ResponseAsync;
        var set = await NodeAsyncAssert.ThrowsAsync<RpcException>(setting, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var getting = client.GetDistributedAsync(new GetDistributedAsyncRequest { CacheName = CacheName, Key = "policy", OperationId = Guid.NewGuid().ToString("N") }, cancellationToken: cancellationToken).ResponseAsync;
        var get = await NodeAsyncAssert.ThrowsAsync<RpcException>(getting, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        var refreshing = client.RefreshDistributedAsync(new RefreshDistributedAsyncRequest { CacheName = CacheName, Key = "policy", OperationId = Guid.NewGuid().ToString("N") }, cancellationToken: cancellationToken).ResponseAsync;
        var refresh = await NodeAsyncAssert.ThrowsAsync<RpcException>(refreshing, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(set.StatusCode).IsEqualTo(StatusCode.Unimplemented);
        _ = await Assert.That(get.StatusCode).IsEqualTo(StatusCode.Unimplemented);
        _ = await Assert.That(refresh.StatusCode).IsEqualTo(StatusCode.Unimplemented);
        var native = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = CacheName, Key = "policy" }, cancellationToken: cancellationToken);
        _ = await Assert.That(native.Found).IsFalse();
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
