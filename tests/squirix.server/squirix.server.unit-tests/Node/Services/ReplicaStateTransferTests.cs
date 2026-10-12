using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.LocalCache;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
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
