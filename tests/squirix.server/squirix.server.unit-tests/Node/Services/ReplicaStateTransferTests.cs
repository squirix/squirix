using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaStateHarness;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A state transfer through the real sink and cache: what the cache holds afterwards, how its bytes are accounted, and the state of the applier.</summary>
[Immutable]
public sealed class ReplicaStateTransferTests : ServerUnitTestBase
{
    private const string Transfer = "transfer-1";

    /// <summary>The keys of the group are replaced by the entries sent, with their deadlines, and the keys of other groups stay.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferReplacesKeysOfTheGroup(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-transfer");
        await using var harness = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        await harness.SeedAsync(cancellationToken, "g-old-1", "g-old-2", "g-gone", "other-1");
        var now = harness.Clock.GetUtcNow().UtcDateTime;
        var first = new[] { Entry("g-new-1", "v1", 0L), Entry("g-new-2", "v2", now.AddHours(1).Ticks) };
        var second = new[] { Entry("g-past", "v3", now.AddHours(-1).Ticks) };
        var totals = TotalsOf([.. first, .. second]);

        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, first, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, second, cancellationToken);
        var end = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(end.Success).IsTrue().Because($"refused with '{end.RefusalCode}'");
        _ = await Assert.That(await ReadAsync(harness, "g-old-1", cancellationToken)).IsNull();
        _ = await Assert.That(await ReadAsync(harness, "g-old-2", cancellationToken)).IsNull();
        _ = await Assert.That(await ReadAsync(harness, "g-gone", cancellationToken)).IsNull().Because("A key absent from the stream is absent afterwards.");
        _ = await Assert.That(await ReadAsync(harness, "other-1", cancellationToken)).IsNotNull().Because("The keys of other groups stay.");
        _ = await Assert.That((await ReadAsync(harness, "g-new-1", cancellationToken))?.ExpiresUtc).IsNull();
        _ = await Assert.That((await ReadAsync(harness, "g-new-2", cancellationToken))?.ExpiresUtc?.Ticks).IsEqualTo(now.AddHours(1).Ticks);
        _ = await Assert.That((await ReadAsync(harness, "g-past", cancellationToken))?.ExpiresUtc?.Ticks).IsEqualTo(now.AddHours(-1).Ticks)
                        .Because("An entry in the stream is stored even when its deadline passed; the replayed record removes it.");
        _ = await Assert.That(BytesOf(harness.Cache)).IsEqualTo(await ReferenceBytesAsync(harness, now, cancellationToken)).Because("The accounted bytes follow the stored entries.");
    }

    /// <summary>The copy ends complete at the transferred index, the applier is reseeded there without a gap report, and the apply driver is released.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplierIsReseededAtTransferredIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-applier");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var entries = new[] { Entry("g-new-1", "v1", 0L) };
        var totals = TotalsOf(entries);

        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, entries, cancellationToken);
        _ = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(harness.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        var status = await log!.GetStatusAsync(cancellationToken);
        _ = await Assert.That((log.IsComplete, status.LastAppliedIndex, status.CommitIndex)).IsEqualTo((true, 3UL, 3UL));
        var applier = harness.Appliers.For(GroupId);
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        await applier.CatchUpAsync(log, status.LastAppliedIndex, status.CommitIndex, cancellationToken);
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        _ = await Assert.That(harness.Events.Count(4027)).IsEqualTo(0).Because("The reseed must not be reported as a gap.");
        _ = await Assert.That(harness.BarrierStates.Count).IsEqualTo(1);
        _ = await Assert.That(harness.BarrierStates[0]).IsEqualTo((GroupMemberState.Incomplete, GroupMemberState.Incomplete)).Because("The barrier comes before the complete record.");
        _ = await Assert.That(applier.DriverLease.TryEnterPass()).IsTrue().Because("The apply driver is released.");
        applier.DriverLease.ExitPass();
        var next = await log.AppendAsync(TransferFixtures.AppendAt(4UL, "d"), cancellationToken);
        _ = await Assert.That((next.Success, next.LastLogIndex)).IsEqualTo((true, 4UL));
    }

    /// <summary>A transfer held by the node holds the apply driver, and the idle timer hands it back and wakes the apply loop.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DriverIsHeldDuringTransfer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-driver");
        await using var harness = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { IdleTimeout = TimeSpan.FromSeconds(30) });
        _ = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        var lease = harness.Appliers.For(GroupId).DriverLease;
        _ = await Assert.That(lease.TryEnterPass()).IsFalse();

        harness.Clock.Advance(TimeSpan.FromSeconds(31));

        _ = await Assert.That(lease.TryEnterPass()).IsTrue();
        lease.ExitPass();
        _ = await Assert.That(await harness.Registry.ApplySignalFor(GroupId).WaitAsync(TimeSpan.FromHours(1), harness.Clock, cancellationToken)).IsTrue();
    }

    /// <summary>A crash after the first wipe step reopens an incomplete copy, and a new transfer wipes the keys memory got back and ends with the right data.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RepeatedTransferWipesAgain(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-crash");
        var crashed = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        await crashed.SeedAsync(cancellationToken, "g-old-1", "g-old-2");
        var sequence = await crashed.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        var step = await crashed.Receiver.WipeAsync(crashed.HeaderFor(Transfer), sequence, cancellationToken);
        _ = await Assert.That((step.Success, step.More)).IsEqualTo((true, true));
        await crashed.DisposeAsync();

        await using var reopened = await OpenAsync(dir, cancellationToken, new ReplicaTransferOptions { WipeStepKeys = 1 });
        await reopened.SeedAsync(cancellationToken, "g-old-1", "g-old-2");
        _ = await Assert.That(reopened.Registry.TryGetLog(GroupId, out var log)).IsTrue();
        _ = await Assert.That((await log!.GetStatusAsync(cancellationToken)).MemberState).IsEqualTo(GroupMemberState.Incomplete);
        var entries = new[] { Entry("g-new-1", "v1", 0L) };
        var totals = TotalsOf(entries);

        sequence = await reopened.BeginAndUploadAsync("transfer-2", 1UL, cancellationToken);
        sequence = await reopened.WipeAllAsync("transfer-2", sequence, cancellationToken);
        sequence = await reopened.SendBatchAsync("transfer-2", sequence, entries, cancellationToken);
        var end = await reopened.Receiver.EndAsync(reopened.HeaderFor("transfer-2"), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(end.Success).IsTrue();
        _ = await Assert.That(log.IsComplete).IsTrue();
        _ = await Assert.That(await ReadAsync(reopened, "g-old-1", cancellationToken)).IsNull();
        _ = await Assert.That(await ReadAsync(reopened, "g-old-2", cancellationToken)).IsNull();
        _ = await Assert.That(await ReadAsync(reopened, "g-new-1", cancellationToken)).IsNotNull();
    }

    /// <summary>A batch with an entry that the locator assigns to another group is refused before any entry of the batch is written, and the receiver ends the transfer on it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EntryOfAnotherGroupIsRefusedUnwritten(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-foreign-entry");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var batch = new[] { Entry("g-new-1", "v1", 0L), Entry("other-1", "v2", 0L) };
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(harness.Sink.WriteAsync(GroupId, batch, cancellationToken));
        var reply = await harness.Receiver.EntriesAsync(harness.HeaderFor(Transfer), sequence, batch, cancellationToken);

        _ = await Assert.That((reply.Success, reply.RefusalCode)).IsEqualTo((false, RefusalCodes.ChecksumMismatch));
        _ = await Assert.That(await ReadAsync(harness, "g-new-1", cancellationToken)).IsNull().Because("Nothing of the batch was written.");
        _ = await Assert.That(await ReadAsync(harness, "other-1", cancellationToken)).IsNull();
    }

    /// <summary>
    /// An entry a transfer stores equals, field by field, the entry a follower stores by applying the records of the same writes: value, version,
    /// tags, absolute deadline, and the relative expiration, which no copy keeps because the leader resolves it into the absolute deadline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferStoresWhatApplyStores(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-apply-equal");
        await using var harness = await OpenAsync(dir, cancellationToken);
        var now = harness.Clock.GetUtcNow().UtcDateTime;
        var physical = new PhysicalCache<object?>(harness.Clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        var follower = new ClientCache<object?>(physical, physical);
        var factory = new ReplicaMutationFactory(follower, "g1", 1UL, harness.Clock, NullLogger.Instance);
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["color"] = "red", ["size"] = "m" }.ToFrozenDictionary(StringComparer.Ordinal);
        var writes = new (string Key, NodeCacheEntry<object?> Entry)[]
        {
            ("k-none", new NodeCacheEntry<object?> { Value = "v1" }),
            ("k-absolute", new NodeCacheEntry<object?> { Value = "v2", ExpiresUtc = now.AddHours(1) }),
            ("k-relative", new NodeCacheEntry<object?> { Value = "v3", Expiration = TimeSpan.FromMinutes(5) }),
            ("k-sliding", new NodeCacheEntry<object?> { Value = "v4", Expiration = TimeSpan.FromMinutes(5) }),
            ("k-tags", new NodeCacheEntry<object?>("v5", 1, now.AddHours(2), TimeSpan.FromMinutes(3), tags)),
        };
        var index = 1UL;
        foreach (var (key, entry) in writes)
            await ApplyAsync(follower, factory.PrepareSet($"op-{key}", CacheName, key, entry, index++), cancellationToken);

        await ApplyAsync(follower, await factory.PrepareTouchAsync("op-touch", CacheName, "k-sliding", TimeSpan.FromMinutes(10), index, cancellationToken), cancellationToken);
        var sent = new ReplicaStateEntry[writes.Length];
        for (var i = 0; i < sent.Length; i++)
        {
            var stored = ThrowHelper.Required(await physical.GetEntryAsync(new CacheKey(CacheName, writes[i].Key), cancellationToken), "The follower must hold the entry.");
            sent[i] = new ReplicaStateEntry(CacheName, Encoding.UTF8.GetBytes(writes[i].Key), ReplicaCacheApplier.EncodeEntry(stored), stored.ExpiresUtc?.Ticks ?? 0L);
        }

        var totals = TotalsOf(sent);
        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, sent, cancellationToken);
        var end = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        _ = await Assert.That(end.Success).IsTrue();
        for (var i = 0; i < writes.Length; i++)
        {
            var applied = await physical.GetEntryAsync(new CacheKey(CacheName, writes[i].Key), cancellationToken);
            var transferred = await ReadAsync(harness, writes[i].Key, cancellationToken);
            _ = await Assert.That(applied).IsNotNull();
            _ = await Assert.That(Describe(transferred)).IsEqualTo(Describe(applied)).Because(writes[i].Key);
        }

        _ = await Assert.That((await physical.GetEntryAsync(new CacheKey(CacheName, "k-sliding"), cancellationToken))?.ExpiresUtc).IsEqualTo(now.AddMinutes(10));
    }

    /// <summary>JSON entries a transfer stores account the bytes the walk of the stored entries measures, whether or not the payload holds a deadline of its own.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferAccountsJsonEntriesLikeAWalk(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-state-transfer-json");
        var estimator = new ObjectCacheEntrySizeEstimator();
        await using var harness = await OpenAsync(dir, cancellationToken, estimator: estimator);
        var deadline = harness.Clock.GetUtcNow().UtcDateTime.AddHours(1);
        var tree = JsonElement.Parse("""{"a":[1,"x",true,null,{"b":2.5}],"name":"héllo"}""");
        var tags = new Dictionary<string, string>(StringComparer.Ordinal) { ["color"] = "red" }.ToFrozenDictionary(StringComparer.Ordinal);
        var entries = new[]
        {
            JsonStateEntry("j-plain", new NodeCacheEntry<object?>(tree), 0L),
            JsonStateEntry("j-deadline", new NodeCacheEntry<object?>(tree, 1, deadline), deadline.Ticks),
            JsonStateEntry("j-tagged", new NodeCacheEntry<object?>(tree, 4, deadline, null, tags), deadline.Ticks),
            JsonStateEntry("j-own-deadline", new NodeCacheEntry<object?>(tree, 2, deadline), 0L),
        };
        var totals = TotalsOf(entries);

        var sequence = await harness.BeginAndUploadAsync(Transfer, 1UL, cancellationToken);
        sequence = await harness.WipeAllAsync(Transfer, sequence, cancellationToken);
        sequence = await harness.SendBatchAsync(Transfer, sequence, entries, cancellationToken);
        var end = await harness.Receiver.EndAsync(harness.HeaderFor(Transfer), sequence, totals.EntryCount, totals.Crc, cancellationToken);

        long walked = 0;
        await foreach (var (key, entry) in harness.Cache.EnumerateLiveAsync(cancellationToken))
            walked += estimator.EstimateBytes(key, entry, false);

        _ = await Assert.That(end.Success).IsTrue().Because($"refused with '{end.RefusalCode}'");
        _ = await Assert.That(walked).IsGreaterThan(0L);
        _ = await Assert.That(BytesOf(harness.Cache)).IsEqualTo(walked);
    }

    private static ReplicaStateEntry JsonStateEntry(string key, NodeCacheEntry<object?> entry, long expiresUtcTicks) =>
        new(CacheName, Encoding.UTF8.GetBytes(key), ReplicaCacheApplier.EncodeEntry(entry), expiresUtcTicks);

    private static string Describe(NodeCacheEntry<object?>? entry)
    {
        if (entry == null)
            return "none";

        var text = new StringBuilder();
        _ = text.Append(entry.Value).Append('|').Append(entry.Version).Append('|').Append(entry.ExpiresUtc?.Ticks).Append('|').Append(entry.Expiration);
        if (entry.Tags == null)
            return text.ToString();
        foreach (var pair in new SortedDictionary<string, string>(entry.Tags, StringComparer.Ordinal))
            _ = text.Append('|').Append(pair.Key).Append('=').Append(pair.Value);
        return text.ToString();
    }

    private static Task ApplyAsync(ILogicalNamespacedCache<object?> cache, PreparedReplicaMutation mutation, CancellationToken cancellationToken) =>
        ReplicaCacheApplier.ApplyAsync(
            cache,
            ReplicaLogCodec.Decode(mutation.CanonicalPayload).GetValueOrDefault(),
            cancellationToken);

    private static long BytesOf(ILocalCacheStats stats) => stats.EstimatedBytes;

    private static ValueTask<NodeCacheEntry<object?>?> ReadAsync(ReplicaStateHarness harness, string key, CancellationToken cancellationToken) =>
        harness.Cache.GetEntryAsync(new CacheKey(CacheName, key), cancellationToken);

    private static async Task<long> ReferenceBytesAsync(ReplicaStateHarness harness, DateTime now, CancellationToken cancellationToken)
    {
        var reference = new PhysicalCache<object?>(harness.Clock, new CacheEntrySizeEstimator<object?>(), expiry: CacheExpiryAuthority.CommittedRecords);
        await reference.SetAsync(new CacheKey(CacheName, "other-1"), new NodeCacheEntry<object?>("old"), cancellationToken);
        await reference.SetAsync(new CacheKey(CacheName, "g-new-1"), new NodeCacheEntry<object?>("v1"), cancellationToken);
        await reference.SetAsync(new CacheKey(CacheName, "g-new-2"), new NodeCacheEntry<object?>("v2", 1, now.AddHours(1)), cancellationToken);
        await reference.SetAsync(new CacheKey(CacheName, "g-past"), new NodeCacheEntry<object?>("v3", 1, now.AddHours(-1)), cancellationToken);
        return BytesOf(reference);
    }
}
