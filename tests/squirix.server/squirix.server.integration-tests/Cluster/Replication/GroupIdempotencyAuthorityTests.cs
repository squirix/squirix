using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>The replica group replays committed write outcomes.</summary>
public sealed class GroupIdempotencyAuthorityTests : NodeIntegrationTestBase
{
    private const string CacheName = "default";
    private const string Owner = "node-a";
    private const int Duplicates = 8;

    /// <summary>A retry of a committed write replays the group outcome without appending another record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetryReplaysGroupOutcome(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-retry"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var request = AddRequest(RpcOperationIdentity.New(), cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "first");

        var first = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(retried.Added).IsTrue();
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>Concurrent duplicates of one write join the first commit: the group log gets one entry and every caller gets its outcome.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConcurrentDuplicatesCommitOnce(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-concurrent"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var request = AddRequest(RpcOperationIdentity.New(), cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "first");
        var before = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var calls = new Task<TryAddAsyncResponse>[Duplicates];
        for (var i = 0; i < calls.Length; i++)
            calls[i] = client.AddEntryIfAbsentAsync(request, cancellationToken: cancellationToken);

        var responses = await Task.WhenAll(calls);

        foreach (var response in responses)
            _ = await Assert.That(response.Added).IsTrue();

        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(before + 1);
    }

    /// <summary>An operation identifier reused with another request is refused by the group as a reuse, and nothing is appended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReuseIsRefusedByGroup(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-mismatch"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var operationId = RpcOperationIdentity.New();
        var key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner);
        _ = await client.AddEntryIfAbsentAsync(AddRequest(operationId, key, "a"), cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var reused = AddRequest(operationId, key, "b");
        var refused = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.AddEntryIfAbsentAsync(reused, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(refused.StatusCode).IsEqualTo(FrameStatus.FailedPrecondition);
        _ = await Assert.That(refused.Message).IsEqualTo(ServerOpIdMismatchException.StableDetail);
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>A retried get-or-add answers with the first outcome, the stored entry, and appends once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddRetryReplaysOutcome(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-get-or-add"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var request = new GetOrAddAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = CacheName,
            Key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner),
            Entry = new NodeCacheEntry<object?> { Value = "first", Version = 1 }.MapToWire(),
        };

        var first = await client.GetOrAddAsync(request, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.GetOrAddAsync(request, cancellationToken: cancellationToken);

        var next = GetOrAddAsyncRequest.Parser.ParseFrom(request.ToByteArray());
        next.OperationId = RpcOperationIdentity.New();
        next.Entry = new NodeCacheEntry<object?> { Value = "second", Version = 1 }.MapToWire();
        var existing = await client.GetOrAddAsync(next, cancellationToken: cancellationToken);

        _ = await Assert.That(retried.ToByteArray().AsSpan().SequenceEqual(first.ToByteArray())).IsTrue();
        _ = await Assert.That(existing.Added).IsFalse();
        _ = await Assert.That(existing.Found).IsTrue();
        _ = await Assert.That(existing.Value!.StringValue).IsEqualTo("first");
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>A retry of a committed touch replays the group outcome without appending another renewal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TouchRetryReplaysGroupOutcome(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-touch"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var key = cluster[Owner].FindKeyOwnedBy(CacheName, Owner);
        _ = await client.AddEntryIfAbsentAsync(AddRequest(RpcOperationIdentity.New(), key, "value"), cancellationToken: cancellationToken);
        var touch = new TouchAsyncRequest { OperationId = RpcOperationIdentity.New(), CacheName = CacheName, Key = key, Expiration = TimeSpan.FromHours(1) };

        var first = await client.TouchAsync(touch, cancellationToken: cancellationToken);
        var committed = await LastLogIndexAsync(cluster[Owner], cancellationToken);
        var retried = await client.TouchAsync(touch, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Found).IsTrue();
        _ = await Assert.That(retried.Found).IsTrue();
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(committed);
    }

    /// <summary>The group tells an operation by its group and cache: the same identifier on another cache is a separate operation, not a reuse.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SameIdOnOtherCacheIsSeparate(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync(Owner, "node-b", "node-c", Options("group-authority-scope"), cancellationToken);
        await ReplicaGroupFollowers.AwaitVerifiedAsync(cluster[Owner], cancellationToken);
        using var channel = CreateBinaryChannel(cluster[Owner].Uri);
        var client = new CacheWireClient(channel);
        var operationId = RpcOperationIdentity.New();
        var before = await LastLogIndexAsync(cluster[Owner], cancellationToken);

        var first = await client.AddEntryIfAbsentAsync(AddRequest(operationId, cluster[Owner].FindKeyOwnedBy(CacheName, Owner), "a"), cancellationToken: cancellationToken);
        var other = AddRequest(operationId, cluster[Owner].FindKeyOwnedBy("other", Owner), "b");
        other.CacheName = "other";
        var second = await client.AddEntryIfAbsentAsync(other, cancellationToken: cancellationToken);

        _ = await Assert.That(first.Added).IsTrue();
        _ = await Assert.That(second.Added).IsTrue();
        _ = await Assert.That(await LastLogIndexAsync(cluster[Owner], cancellationToken)).IsEqualTo(before + 2);
    }

    private static IntegrationStartOptions Options(string scope) => new() { ReplicaCount = 3, UsePersistence = true, CleanTestDir = true, ExtraScope = scope };

    private static TryAddEntryAsyncRequest AddRequest(string operationId, string key, string value) => new()
    {
        OperationId = operationId,
        CacheName = CacheName,
        Key = key,
        Entry = new NodeCacheEntry<object?> { Value = value, Version = 1 }.MapToWire(),
    };

    private static async Task<ulong> LastLogIndexAsync(ITestNodeHost node, CancellationToken cancellationToken)
    {
        _ = node.GetRequiredService<ReplicaGroupRegistry>().TryGetLog(Owner, out var log);
        return (await log!.GetStatusAsync(cancellationToken)).LastLogIndex;
    }
}
