using System;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.Services;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaExpiryKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The expiry of several keys under one hold of the commit gate: one decision per key on the leader clock, one run, one majority.</summary>
public sealed class ReplicaBatchExpiryTests : ServerUnitTestBase
{
    private static int BatchLimit => ReplicaGroupCommitterCommits.MaxExpiriesPerCommit;

    /// <summary>The batch limit fits the acknowledgement buffer of a follower slot and the backlog of a sender.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task BatchLimitFitsReplicationBuffers(CancellationToken cancellationToken)
    {
        var limit = BatchLimit;
        _ = await Assert.That(limit).IsLessThanOrEqualTo(ReplicaCommitQuorum.MaxBufferedAcks);
        _ = await Assert.That(limit).IsLessThanOrEqualTo(ReplicaFollowerSender.DefaultMaxPendingEntries);

        using var dir = new TempDirectory("squirix-batch-expiry-limit");
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), new FakeTimeProvider(Start));
        var tooMany = KeysOf(ReplicaGroupCommitterCommits.MaxExpiriesPerCommit + 1);

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException, ReplicaExpiryResult[]>(new ValueTask<ReplicaExpiryResult[]>(leader.Committer.CommitExpiriesAsync(tooMany)));
    }

    /// <summary>A batch that finds nothing to tombstone appends nothing.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task BatchOfLiveAndAbsentKeysAppendsNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-nothing");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        var appended = leader.Committer.LocalAppends.Snapshot().Appended;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "live"), (CacheName, "gone")]);

        _ = await Assert.That(results[0].Entry?.Value).IsEqualTo("v");
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Absent);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before);
        _ = await Assert.That(leader.Committer.LocalAppends.Snapshot().Appended).IsEqualTo(appended);
    }

    /// <summary>A burst of keys passed in chunks of the limit uses one local append and at most one request per follower for each chunk.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task BurstUsesOneAppendPerBatch(CancellationToken cancellationToken)
    {
        const int burst = ReplicaGroupCommitterCommits.MaxExpiriesPerCommit + 6;
        const int chunks = 2;
        const int followers = 2;
        using var dir = new TempDirectory("squirix-batch-expiry-burst");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        var keys = KeysOf(burst);
        await SeedExpiredAsync(leader, clock, NamesOf(keys), cancellationToken);
        var appended = leader.Committer.LocalAppends.Snapshot().Appended;
        var sent = gateway.Appends.Count;

        var first = await leader.Committer.CommitExpiriesAsync(keys[..ReplicaGroupCommitterCommits.MaxExpiriesPerCommit]);
        var second = await leader.Committer.CommitExpiriesAsync(keys[ReplicaGroupCommitterCommits.MaxExpiriesPerCommit..]);

        _ = await Assert.That(first.Length + second.Length).IsEqualTo(burst);
        _ = await Assert.That(leader.Committer.LocalAppends.Snapshot().Appended - appended).IsEqualTo(ulong.CreateChecked(chunks));
        _ = await Assert.That(gateway.Appends.Count - sent).IsLessThanOrEqualTo(chunks * followers);
        _ = await Assert.That(await CountExpireRecordsAsync(registry, cancellationToken)).IsEqualTo(burst);
        _ = await Assert.That(await leader.RawAsync(keys[0].Key, cancellationToken)).IsNull();
        _ = await Assert.That(await leader.RawAsync(keys[^1].Key, cancellationToken)).IsNull();
    }

    /// <summary>A client write queued on the commit gate between two batches runs between them, in queue order.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task ClientWriteRunsBetweenBatches(CancellationToken cancellationToken)
    {
        const int firstBatch = 3;
        const int secondBatch = 2;
        using var dir = new TempDirectory("squirix-batch-expiry-between");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ReplicaCommitterDoubles.ParkingGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        await SeedExpiredAsync(leader, clock, ["a", "b", "c", "d", "e"], cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        gateway.ArmForEntries();
        try
        {
            var first = leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "b"), (CacheName, "c")]);
            await gateway.Entered.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            var write = leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "client", new NodeCacheEntry<object?>("w"), cancellationToken);
            var second = leader.Committer.CommitExpiriesAsync([(CacheName, "d"), (CacheName, "e")]);

            gateway.Release();
            _ = await first.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            await write.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            _ = await second.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
        }

        var records = await RecordsAsync(registry, cancellationToken);
        var start = int.CreateChecked(before);
        for (var i = 0; i < firstBatch; i++)
            _ = await Assert.That(records[start + i].MutationKind).IsEqualTo(ReplicaMutationKinds.Expire);

        _ = await Assert.That(records[start + firstBatch].MutationKind).IsEqualTo(ReplicaMutationKinds.Set);
        for (var i = 0; i < secondBatch; i++)
            _ = await Assert.That(records[start + firstBatch + 1 + i].MutationKind).IsEqualTo(ReplicaMutationKinds.Expire);

        _ = await Assert.That(records.Count).IsEqualTo(start + firstBatch + 1 + secondBatch);
    }

    /// <summary>A key given twice is decided and committed once, and both positions get its result; absent keys get no record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task DuplicateAndAbsentKeysAreSkipped(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-duplicates");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b"], cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "gone"), (CacheName, "a"), (CacheName, "b")]);

        _ = await Assert.That(results.Length).IsEqualTo(4);
        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Absent);
        _ = await Assert.That(results[2].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(results[3].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 2);
        _ = await Assert.That(await CountExpireRecordsAsync(registry, cancellationToken)).IsEqualTo(2);
    }

    /// <summary>A live key given twice gets its stored entry at both positions and no record.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task DuplicateLiveKeyGetsEntryAtBothPositions(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-duplicate-live");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "live"), (CacheName, "live")]);

        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[0].Entry?.Value).IsEqualTo("v");
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[1].Entry?.Value).IsEqualTo("v");
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before);
    }

    /// <summary>A key rewritten after the caller scanned it is returned live and gets no tombstone, while the other expired key does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task KeyRewrittenAfterScanIsNotTombstoned(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-rewritten");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b"], cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "b", new NodeCacheEntry<object?>("new"), cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "b")]);

        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[1].Entry?.Value).IsEqualTo("new");
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 1);
        _ = await Assert.That(await leader.RawAsync("a", cancellationToken)).IsNull();
        _ = await Assert.That((await leader.RawAsync("b", cancellationToken))?.Value).IsEqualTo("new");
    }

    /// <summary>A live key placed before and between expired keys takes no log index: the tombstones stay consecutive.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task LiveKeysBeforeTombstonesKeepIndexesDense(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-dense");
        var clock = new FakeTimeProvider(Start);
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, new ScriptedGateway(), clock);
        await SeedExpiredAsync(leader, clock, ["a", "b"], cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;

        var results = await leader.Committer.CommitExpiriesAsync([(CacheName, "live"), (CacheName, "a"), (CacheName, "live"), (CacheName, "b")]);

        var records = await RecordsAsync(registry, cancellationToken);
        _ = await Assert.That(results[0].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(results[3].Status).IsEqualTo(ReplicaExpiryStatus.Tombstoned);
        _ = await Assert.That(records.Count).IsEqualTo(int.CreateChecked(before) + 2);
        _ = await Assert.That(records[int.CreateChecked(before)].LogIndex).IsEqualTo(before + 1);
        _ = await Assert.That(records[int.CreateChecked(before) + 1].LogIndex).IsEqualTo(before + 2);
        _ = await Assert.That(await leader.RawAsync("live", cancellationToken)).IsNotNull();
    }

    /// <summary>
    /// Without a majority the outcome of the run is unknown for its keys alone, while the live and absent keys of the batch keep their
    /// result; the same batch asked again is refused retryably while the appended entries are unapplied, before anything is appended and
    /// without consuming a log index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task UnknownOutcomeKeepsDecidedKeys(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-unknown");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        await SeedExpiredAsync(leader, clock, ["a", "b", "c"], cancellationToken);
        await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "live", new NodeCacheEntry<object?>("v", 1, null, Ttl), cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        (string CacheName, string Key)[] keys = [(CacheName, "a"), (CacheName, "live"), (CacheName, "b"), (CacheName, "gone"), (CacheName, "c")];

        var results = await leader.Committer.CommitExpiriesAsync(keys);
        var written = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, ReplicaExpiryResult[]>(new ValueTask<ReplicaExpiryResult[]>(leader.Committer.CommitExpiriesAsync(keys)));

        foreach (var position in new[] { 0, 2, 4 })
        {
            _ = await Assert.That(results[position].Status).IsEqualTo(ReplicaExpiryStatus.Failed);
            _ = await Assert.That((results[position].Failure as SquirixException)?.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        }

        _ = await Assert.That(results[1].Status).IsEqualTo(ReplicaExpiryStatus.Live);
        _ = await Assert.That(results[1].Entry?.Value).IsEqualTo("v");
        _ = await Assert.That(results[3].Status).IsEqualTo(ReplicaExpiryStatus.Absent);
        _ = await Assert.That(written).IsEqualTo(before + 3);
        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(written);
        _ = await Assert.That((await leader.RawAsync("a", cancellationToken))?.Value).IsEqualTo("a");
    }
}
