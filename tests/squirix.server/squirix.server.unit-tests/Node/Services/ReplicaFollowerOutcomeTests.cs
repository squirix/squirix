using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
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

/// <summary>
/// A follower group keeps the idempotency outcomes of its log: the recovered uncommitted tail is pinned when the group opens, a
/// truncation releases the pins of the entries it drops, and the apply of a committed entry records its outcome once the outcomes are rebuilt.
/// </summary>
public sealed class ReplicaFollowerOutcomeTests : ServerUnitTestBase
{
    private const string GroupId = "n2";

    private static readonly string[] Groups = ["n1", "n2", "n3"];

    /// <summary>
    /// A catch-up before the outcomes of the log are rebuilt applies every entry and records nothing; the rebuild then restores the
    /// outcome of each of them from the log.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyBeforeRebuildRecordsNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-before");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var records = new[] { Prepare("k1", 1UL), Prepare("k2", 2UL) };
        await AppendAsync(log, 0UL, 1UL, [Entry(in records[0]), Entry(in records[1])], 2UL, cancellationToken);
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1");

        await applier.CatchUpAsync(log, 0UL, 2UL, cancellationToken);

        _ = await Assert.That((applier.AppliedIndex, cache.Applied.Count)).IsEqualTo((2UL, 2));
        _ = await Assert.That(Lookup(log, in records[0])).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(log, in records[1])).IsEqualTo(GroupIdempotencyLookup.Miss);

        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);

        _ = await Assert.That(Lookup(log, in records[0])).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(log, in records[1])).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>A catch-up after the outcomes of the log are rebuilt records the outcome of every entry it applies.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyAfterRebuildRecordsOutcomes(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-after");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var records = new[] { Prepare("k1", 1UL), Prepare("k2", 2UL) };
        log.Idempotency.MarkOutcomesRebuilt();
        await AppendAsync(log, 0UL, 1UL, [Entry(in records[0]), Entry(in records[1])], 2UL, cancellationToken);
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1");

        await applier.CatchUpAsync(log, 0UL, 2UL, cancellationToken);

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That(Lookup(log, in records[0])).IsEqualTo(GroupIdempotencyLookup.Found);
        _ = await Assert.That(Lookup(log, in records[1])).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    /// <summary>
    /// The apply of an entry whose outcome a coordinator already resolved records it again without changing it: the store keeps one
    /// record with the outcome the entry carries.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyKeepsResolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-resolved-twice");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var record = Prepare("k1", 1UL);
        log.Idempotency.MarkOutcomesRebuilt();
        await AppendAsync(log, 0UL, 1UL, [Entry(in record)], 1UL, cancellationToken);
        _ = log.Idempotency.Reserve(record.OperationScope, record.OperationId, record.OperationFingerprint.Span, GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = log.Idempotency.TryResolve(record.OperationScope, record.OperationId, record.OutcomePayload.Span, 1UL, 1UL);
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1");

        await applier.CatchUpAsync(log, 0UL, 1UL, cancellationToken);

        _ = await Assert.That(Lookup(log, in record, out var outcome)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync(record.OutcomePayload.ToArray(), outcome.OutcomePayload.ToArray());
        _ = await Assert.That(log.Idempotency.ExportResolved(out _).Count).IsEqualTo(1);
    }

    /// <summary>The apply of a pinned entry that commits after the restart resolves its pin with the outcome its record carries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedPinIsResolvedByApply(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-resolved");
        var records = await SeedTailAsync(dir, cancellationToken, "k1", "k2");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        _ = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);
        await AppendAsync(log, 2UL, 1UL, [], 2UL, cancellationToken);
        var applier = new ReplicaGroupApplier(new StubCache(), NullLogger.Instance, GroupId, "n1");

        await applier.CatchUpAsync(log, 0UL, 2UL, cancellationToken);

        _ = await Assert.That(Lookup(log, in records[1], out var outcome)).IsEqualTo(GroupIdempotencyLookup.Found);
        await SequenceAssert.EqualAsync(records[1].OutcomePayload.ToArray(), outcome.OutcomePayload.ToArray());
        _ = await Assert.That(log.Idempotency.HasUnresolvedThrough(2UL)).IsFalse();
    }

    /// <summary>The uncommitted entries a follower group log holds at open are pinned before the group is published.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoveredTailIsPinned(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-pinned");
        var records = await SeedTailAsync(dir, cancellationToken, "k1", "k2");

        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);

        _ = await Assert.That(Lookup(log, in records[0])).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(Lookup(log, in records[1])).IsEqualTo(GroupIdempotencyLookup.Unresolved);
        _ = await Assert.That(log.Idempotency.HasUnresolvedThrough(1UL)).IsTrue();
    }

    /// <summary>A truncation of the recovered tail releases the pins of the entries it drops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncationReleasesRecoveredPin(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-truncated");
        var records = await SeedTailAsync(dir, cancellationToken, "k1", "k2");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var replacement = Prepare("k9", 1UL) with { Term = 2UL };

        await AppendAsync(log, 0UL, 2UL, [new FollowerLogEntry(1UL, 2UL, ReplicaLogCodec.Encode(in replacement))], 0UL, cancellationToken);

        _ = await Assert.That(Lookup(log, in records[0])).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(log, in records[1])).IsEqualTo(GroupIdempotencyLookup.Miss);
    }

    /// <summary>A tail entry whose record cannot be read is left unpinned and does not keep the group from opening.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnreadableTailEntryIsNotPinned(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-outcome-unreadable");
        await using (var seed = await OpenRegistryAsync(dir, Groups, null, cancellationToken))
        {
            FollowerLogEntry[] entries = [new(1UL, 1UL, new byte[] { 1, 2, 3 })];
            await AppendAsync(LogOf(seed), 0UL, 1UL, entries, 0UL, cancellationToken);
        }

        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);

        _ = await Assert.That(LogOf(registry).Idempotency.HasUnresolvedThrough(1UL)).IsFalse();
    }

    /// <summary>Appends entries to a group log after <paramref name="prevLogIndex" /> in term one, failing the test when the log refuses them.</summary>
    /// <param name="log">The group log.</param>
    /// <param name="prevLogIndex">The index of the entry before the batch.</param>
    /// <param name="term">The leader term of the append.</param>
    /// <param name="entries">The entries.</param>
    /// <param name="commitIndex">The leader commit index.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>An asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException">The log refused the append.</exception>
    private static async Task AppendAsync(IFollowerLog log, ulong prevLogIndex, ulong term, FollowerLogEntry[] entries, ulong commitIndex, CancellationToken cancellationToken)
    {
        var appended = await log.AppendAsync(new FollowerLogAppendRequest(GroupId, term, prevLogIndex, prevLogIndex == 0 ? 0UL : 1UL, commitIndex, entries), cancellationToken);
        if (!appended.Success)
            throw new InvalidOperationException($"The group log refused the append: {appended.RefusalCode}.");
    }

    private static FollowerLogEntry Entry(in ReplicaLogRecord record) => new(record.LogIndex, record.Term, ReplicaLogCodec.Encode(in record));

    private static IFollowerLog LogOf(ReplicaGroupRegistry registry) =>
        registry.TryGetLog(GroupId, out var log) ? log : throw new InvalidOperationException($"The group log {GroupId} is not open.");

    private static GroupIdempotencyLookup Lookup(IFollowerLog log, in ReplicaLogRecord record) => Lookup(log, in record, out _);

    private static GroupIdempotencyLookup Lookup(IFollowerLog log, in ReplicaLogRecord record, out GroupIdempotencyRecord outcome) => log.Idempotency.Lookup(
        record.OperationScope,
        record.OperationId,
        record.OperationFingerprint.Span,
        out outcome);

    /// <summary>Prepares a record that sets <paramref name="key" /> at <paramref name="logIndex" /> in term one.</summary>
    /// <param name="key">The key the record writes.</param>
    /// <param name="logIndex">The log index of the record.</param>
    /// <returns>The decoded record.</returns>
    /// <exception cref="InvalidOperationException">The prepared record does not decode.</exception>
    private static ReplicaLogRecord Prepare(string key, ulong logIndex)
    {
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        var prepared = factory.PrepareSet(NewOperationId(), "cache", key, ReplicaOwnerTestKit.Entry(key), logIndex);
        return ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record does not decode."));
    }

    /// <summary>Appends uncommitted records of the given keys to the follower group log, then closes it, as a follower that stopped before they committed.</summary>
    /// <param name="dir">Node data directory.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <param name="keys">The keys, in log order from index 1.</param>
    /// <returns>The appended records.</returns>
    private static async Task<ReplicaLogRecord[]> SeedTailAsync(string dir, CancellationToken cancellationToken, params string[] keys)
    {
        var records = new ReplicaLogRecord[keys.Length];
        var entries = new FollowerLogEntry[keys.Length];
        for (var i = 0; i < keys.Length; i++)
        {
            records[i] = Prepare(keys[i], ulong.CreateChecked(i + 1));
            entries[i] = Entry(in records[i]);
        }

        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        await AppendAsync(LogOf(registry), 0UL, 1UL, entries, 0UL, cancellationToken);
        return records;
    }
}
