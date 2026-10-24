using System;
using System.Collections.Generic;
using System.Globalization;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>The expiry of several keys under one hold of the commit gate: one decision per key on the leader clock, one run, one majority.</summary>
public sealed class ReplicaBatchExpiryTests : ServerUnitTestBase
{
    private const string CacheName = "cache";
    private static readonly DateTimeOffset Start = new(2030, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly TimeSpan Ttl = TimeSpan.FromSeconds(60);

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

        _ = await NodeAsyncAssert.ThrowsAsync<ArgumentOutOfRangeException, NodeCacheEntry<object?>?[]>(new ValueTask<NodeCacheEntry<object?>?[]>(leader.Committer.CommitExpiriesAsync(tooMany)));
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

        _ = await Assert.That(results[0]).IsNull();
        _ = await Assert.That(results[1]?.Value).IsEqualTo("new");
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 1);
        _ = await Assert.That(await leader.RawAsync("a", cancellationToken)).IsNull();
        _ = await Assert.That((await leader.RawAsync("b", cancellationToken))?.Value).IsEqualTo("new");
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
        _ = await Assert.That(AllNull(results)).IsTrue();
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before + 2);
        _ = await Assert.That(await CountExpireRecordsAsync(registry, cancellationToken)).IsEqualTo(2);
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

        _ = await Assert.That(results[0]?.Value).IsEqualTo("v");
        _ = await Assert.That(results[1]).IsNull();
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(before);
        _ = await Assert.That(leader.Committer.LocalAppends.Snapshot().Appended).IsEqualTo(appended);
    }

    /// <summary>A burst of keys passed in chunks of the limit uses one local append and at most one request per follower for each chunk.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task BurstUsesOneAppendPerBatch(CancellationToken cancellationToken)
    {
        const int Burst = ReplicaGroupCommitterCommits.MaxExpiriesPerCommit + 6;
        const int Chunks = 2;
        using var dir = new TempDirectory("squirix-batch-expiry-burst");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        var keys = KeysOf(Burst);
        await SeedExpiredAsync(leader, clock, NamesOf(keys), cancellationToken);
        var appended = leader.Committer.LocalAppends.Snapshot().Appended;
        var sent = gateway.Appends.Count;

        var first = await leader.Committer.CommitExpiriesAsync(keys[..ReplicaGroupCommitterCommits.MaxExpiriesPerCommit]);
        var second = await leader.Committer.CommitExpiriesAsync(keys[ReplicaGroupCommitterCommits.MaxExpiriesPerCommit..]);

        _ = await Assert.That(first.Length + second.Length).IsEqualTo(Burst);
        _ = await Assert.That(leader.Committer.LocalAppends.Snapshot().Appended - appended).IsEqualTo(ulong.CreateChecked(Chunks));
        _ = await Assert.That(gateway.Appends.Count - sent).IsLessThanOrEqualTo(Chunks * 2);
        _ = await Assert.That(await CountExpireRecordsAsync(registry, cancellationToken)).IsEqualTo(Burst);
        _ = await Assert.That(await leader.RawAsync(keys[0].Key, cancellationToken)).IsNull();
        _ = await Assert.That(await leader.RawAsync(keys[^1].Key, cancellationToken)).IsNull();
    }

    /// <summary>A client write queued on the commit gate while a batch runs takes the next log index after the whole batch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task ClientWriteRunsBetweenBatches(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-between");
        var clock = new FakeTimeProvider(Start);
        var gateway = new HoldingGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        await SeedExpiredAsync(leader, clock, ["a", "b", "c"], cancellationToken);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        gateway.Hold();
        try
        {
            var batch = leader.Committer.CommitExpiriesAsync([(CacheName, "a"), (CacheName, "b"), (CacheName, "c")]);
            await gateway.Entered.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            var write = leader.Committer.CommitSetAsync(NewOperationId(), CacheName, "client", new NodeCacheEntry<object?>("w"), cancellationToken);

            gateway.Release();
            _ = await batch.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
            await write.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        }
        finally
        {
            gateway.Release();
        }

        var records = await RecordsAsync(registry, cancellationToken);
        _ = await Assert.That(records[^1].LogIndex).IsEqualTo(before + 4);
        _ = await Assert.That(records[^1].MutationKind).IsEqualTo(ReplicaMutationKinds.Set);
        _ = await Assert.That(records[^2].MutationKind).IsEqualTo(ReplicaMutationKinds.Expire);
        _ = await Assert.That(records[int.CreateChecked(before)].MutationKind).IsEqualTo(ReplicaMutationKinds.Expire);
    }

    /// <summary>
    /// Without a majority the outcome of the batch is unknown, and the same batch asked again is refused retryably while the appended entries
    /// are unapplied, before anything is appended and without consuming a log index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the test has run.</returns>
    [Test]
    public async Task UnknownOutcomeThenRefusalConsumesNoIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-batch-expiry-unknown");
        var clock = new FakeTimeProvider(Start);
        var gateway = new ScriptedGateway();
        await using var registry = await OpenRegistryAsync(dir, cancellationToken);
        await using var leader = new Leader(registry, gateway, clock);
        await SeedExpiredAsync(leader, clock, ["a", "b", "c"], cancellationToken);
        gateway.Set("n2", FollowerMode.Down);
        gateway.Set("n3", FollowerMode.Down);
        var before = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        (string CacheName, string Key)[] keys = [(CacheName, "a"), (CacheName, "b"), (CacheName, "c")];

        var unknown = await NodeAsyncAssert.ThrowsAsync<SquirixException, NodeCacheEntry<object?>?[]>(new ValueTask<NodeCacheEntry<object?>?[]>(leader.Committer.CommitExpiriesAsync(keys)));
        var written = (await StatusAsync(registry, cancellationToken)).LastLogIndex;
        var refused = await NodeAsyncAssert.ThrowsAsync<RpcException, NodeCacheEntry<object?>?[]>(new ValueTask<NodeCacheEntry<object?>?[]>(leader.Committer.CommitExpiriesAsync(keys)));

        _ = await Assert.That(unknown.Detail).IsEqualTo(ServerOpContract.CommitOutcomeUnknownDetail);
        _ = await Assert.That(written).IsEqualTo(before + 3);
        _ = await Assert.That(refused.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That((await StatusAsync(registry, cancellationToken)).LastLogIndex).IsEqualTo(written);
        _ = await Assert.That((await leader.RawAsync("a", cancellationToken))?.Value).IsEqualTo("a");
    }

    private static (string CacheName, string Key)[] KeysOf(int count)
    {
        var keys = new (string CacheName, string Key)[count];
        for (var i = 0; i < count; i++)
            keys[i] = (CacheName, "key-" + i.ToString(CultureInfo.InvariantCulture));

        return keys;
    }

    private static string[] NamesOf((string CacheName, string Key)[] keys)
    {
        var names = new string[keys.Length];
        for (var i = 0; i < keys.Length; i++)
            names[i] = keys[i].Key;

        return names;
    }

    private static bool AllNull(NodeCacheEntry<object?>?[] results)
    {
        for (var i = 0; i < results.Length; i++)
        {
            if (results[i] != null)
                return false;
        }

        return true;
    }

    private static async Task SeedExpiredAsync(Leader leader, FakeTimeProvider clock, string[] keys, CancellationToken cancellationToken)
    {
        foreach (var key in keys)
            await leader.Committer.CommitSetAsync(NewOperationId(), CacheName, key, new NodeCacheEntry<object?>(key, 1, null, Ttl), cancellationToken);

        clock.Advance(Ttl);
    }

    private static async Task<int> CountExpireRecordsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var records = await RecordsAsync(registry, cancellationToken);
        var count = 0;
        for (var i = 0; i < records.Count; i++)
            count += string.Equals(records[i].MutationKind, ReplicaMutationKinds.Expire, StringComparison.Ordinal) ? 1 : 0;

        return count;
    }

    private static async Task<List<ReplicaLogRecord>> RecordsAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        if (!registry.TryGetLog("n1", out var log))
            throw new InvalidOperationException("The owned group log is not open.");

        var read = await log.ReadEntriesAsync(1, 256, cancellationToken);
        var records = new List<ReplicaLogRecord>(read.Entries.Count);
        foreach (var entry in read.Entries)
            records.Add(ReplicaLogCodec.Decode(entry.Payload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The log entry must decode.")));

        return records;
    }

    /// <summary>Follower double that accepts every batch; once held, it parks the batches that carry entries until released.</summary>
    [ThreadSafe]
    private sealed class HoldingGateway : IReplicaRpcGateway
    {
        private readonly TaskCompletionSource _entered = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _released = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _held;

        internal Task Entered => _entered.Task;

        public async Task<FollowerLogAppendResult> AppendEntriesAsync(string nodeId, ReplicaRpcHeader header, FollowerBatch batch, CancellationToken cancellationToken)
        {
            if (batch.Records.Count > 0 && Volatile.Read(ref _held) != 0)
            {
                _ = _entered.TrySetResult();
                await new ValueTask(_released.Task).ConfigureAwait(false);
            }

            var last = batch.Records.Count == 0 ? batch.PrevLogIndex : batch.Records[^1].LogIndex;
            return new FollowerLogAppendResult(true, string.Empty, batch.LeaderTerm, last);
        }

        internal void Hold() => Volatile.Write(ref _held, 1);

        internal void Release() => _ = _released.TrySetResult();
    }

    /// <summary>The owner of group n1 over an open registry: a committed-records cache and a committer on one fake clock.</summary>
    private sealed class Leader : IAsyncDisposable
    {
        internal Leader(ReplicaGroupRegistry registry, IReplicaRpcGateway gateway, FakeTimeProvider clock)
        {
            Physical = new PhysicalCache<object?>(clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
            var local = new ClientCache<object?>(Physical, Physical);
            Committer = CreateCommitter(registry, gateway, local, clock);
        }

        internal ReplicaGroupCommitter Committer { get; }

        internal PhysicalCache<object?> Physical { get; }

        public ValueTask DisposeAsync() => Committer.DisposeAsync();

        internal ValueTask<NodeCacheEntry<object?>?> RawAsync(string key, CancellationToken cancellationToken) =>
            Physical.RawReader.GetEntryRawAsync(new CacheKey(CacheName, key), cancellationToken);
    }
}
