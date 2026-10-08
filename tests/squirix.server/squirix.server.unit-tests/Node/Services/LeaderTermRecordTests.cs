using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Time.Testing;
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
/// The no-op a leader commits for its term travels as an ordinary log record: it keeps its kind through the codec and a snapshot, changes
/// no cache, and answers no client retry, so it never holds an outcome or a place in the idempotency store.
/// </summary>
public sealed class LeaderTermRecordTests : ServerUnitTestBase
{
    private const string GroupId = "n2";

    private static readonly string[] Groups = ["n1", "n2", "n3"];
    private static readonly byte[] Key = [107];

    /// <summary>The prepared no-op decodes to the leader-term identity of its group and term and resolves to no effect.</summary>
    [Test]
    public async Task CodecRoundTripKeepsIdentity()
    {
        var record = PrepareNoop(7UL, 3UL);

        _ = await Assert.That(record.LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(record.Term).IsEqualTo(7UL);
        _ = await Assert.That(record.OperationId).IsEqualTo("term-7");
        _ = await Assert.That(record.OperationScope).IsEqualTo(ReplicaLeaderOperationId.OperationScope);
        _ = await Assert.That(record.RecordKind).IsEqualTo(nameof(GroupRecordKind.LeaderTerm));
        _ = await Assert.That(record.MutationKind).IsEqualTo(ReplicaMutationKinds.LeaderNoop);
        await SequenceAssert.EqualAsync(ReplicaLeaderOperationId.Fingerprint(GroupId, 7UL), record.OperationFingerprint.ToArray());
        _ = await Assert.That(GroupRecordKinds.FromScope(record.OperationScope)).IsEqualTo(GroupRecordKind.LeaderTerm);
        _ = await Assert.That(ReplicaCacheApplier.ResolveEffect(in record)).IsEqualTo(ReplicaEffectKind.NoCacheEffect);
    }

    /// <summary>The fingerprint separates groups and terms, so no two no-ops share an identity by accident.</summary>
    [Test]
    public async Task FingerprintSeparatesGroupAndTerm()
    {
        var reference = ReplicaLeaderOperationId.Fingerprint("n1", 2UL);

        await SequenceAssert.EqualAsync(reference, ReplicaLeaderOperationId.Fingerprint("n1", 2UL));
        _ = await Assert.That(reference.AsSpan().SequenceEqual(ReplicaLeaderOperationId.Fingerprint("n2", 2UL))).IsFalse();
        _ = await Assert.That(reference.AsSpan().SequenceEqual(ReplicaLeaderOperationId.Fingerprint("n1", 3UL))).IsFalse();
    }

    /// <summary>A no-op that names a key is refused before anything is applied.</summary>
    [Test]
    public async Task MalformedNoopIsRefused()
    {
        var keyed = PrepareNoop(2UL, 1UL) with { KeyPayload = Key };

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(keyed, static record => ReplicaCacheApplier.ResolveShape(in record));
        _ = await Assert.That(ReplicaCacheApplier.ResolveShape(PrepareNoop(2UL, 1UL))).IsEqualTo(ReplicaEffectKind.NoCacheEffect);
    }

    /// <summary>A snapshot keeps the leader-term kind of a record through its wire value.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SnapshotKeepsLeaderTermKind(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-term-snapshot");
        await (await OpenRegistryAsync(dir, Groups, null, cancellationToken)).DisposeAsync();
        var store = new GroupSnapshotStore(dir, GroupId);
        var record = new GroupIdempotencyRecord(
            ReplicaLeaderOperationId.OperationScope,
            "term-2",
            new byte[] { 1 },
            new byte[] { 0, 0, 0, 0, 0 },
            GroupRecordKind.LeaderTerm,
            DateTime.UnixEpoch,
            DateTime.UnixEpoch,
            1UL,
            2UL);

        await store.PublishAsync(new GroupSnapshot(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 2UL, 1UL, 1UL, [record], DateTime.UnixEpoch), cancellationToken);
        var published = await store.ReadPublishedAsync(cancellationToken);

        _ = await Assert.That(published).IsNotNull();
        _ = await Assert.That(published!.Value.CommittedOutcomes.Count).IsEqualTo(1);
        _ = await Assert.That(published.Value.CommittedOutcomes[0].Kind).IsEqualTo(GroupRecordKind.LeaderTerm);
    }

    /// <summary>A pinned no-op bypasses the capacity while in flight and leaves no outcome once it resolves.</summary>
    [Test]
    public async Task NoopBypassesCapacity()
    {
        var state = new GroupIdempotencyState(1, TimeSpan.FromHours(1), new FakeTimeProvider(DateTimeOffset.UtcNow));
        _ = state.Reserve("client", "first", [1], GroupRecordKind.UserMutation, 1UL, 1UL);
        _ = state.TryResolve("client", "first", [2], 1UL, 1UL);

        var reserved = state.Reserve(ReplicaLeaderOperationId.OperationScope, "term-1", [3], GroupRecordKind.LeaderTerm, 2UL, 1UL);
        var replayed = state.Reserve(ReplicaLeaderOperationId.OperationScope, "term-1", [3], GroupRecordKind.LeaderTerm, 2UL, 1UL);
        var resolved = state.TryResolve(ReplicaLeaderOperationId.OperationScope, "term-1", [4], 2UL, 1UL);

        _ = await Assert.That(reserved).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(replayed).IsEqualTo(GroupIdempotencyReserveResult.Success);
        _ = await Assert.That(resolved).IsTrue();
        _ = await Assert.That(state.Lookup(ReplicaLeaderOperationId.OperationScope, "term-1", [3], out _)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(state.ExportResolved(out _).Count).IsEqualTo(1);
    }

    /// <summary>The apply of a committed no-op touches no cache, records no outcome, drops its pin, and moves the applied index past it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ApplyChangesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-term-apply");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var noop = PrepareNoop(1UL, 1UL);
        await AppendCommittedAsync(log, [noop], cancellationToken);
        _ = log.Idempotency.Reserve(noop.OperationScope, noop.OperationId, noop.OperationFingerprint.Span, GroupRecordKind.LeaderTerm, 1UL, 1UL, true);
        log.Idempotency.MarkOutcomesRebuilt();
        var cache = new StubCache();
        var applier = new ReplicaGroupApplier(cache, NullLogger.Instance, GroupId, "n1") { RecordsOutcomes = true };

        await applier.CatchUpAsync(log, 0UL, 1UL, cancellationToken);

        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(1UL);
        _ = await Assert.That(cache.Applied.Count).IsEqualTo(0);
        _ = await Assert.That(Lookup(log, in noop)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(log.Idempotency.HasUnresolvedThrough(1UL)).IsFalse();
    }

    /// <summary>The outcome rebuild after a restart reads past a committed no-op and restores only the client outcomes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RebuildSkipsNoop(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-leader-term-rebuild");
        await using var registry = await OpenRegistryAsync(dir, Groups, null, cancellationToken);
        var log = LogOf(registry);
        var noop = PrepareNoop(1UL, 1UL);
        var write = PrepareSet("k1", 2UL);
        await AppendCommittedAsync(log, [noop, write], cancellationToken);

        var restored = await ReplicaOutcomeRecovery.RestoreAsync(log, TimeProvider.System, cancellationToken);

        _ = await Assert.That(restored).IsEqualTo(1);
        _ = await Assert.That(Lookup(log, in noop)).IsEqualTo(GroupIdempotencyLookup.Miss);
        _ = await Assert.That(Lookup(log, in write)).IsEqualTo(GroupIdempotencyLookup.Found);
    }

    private static async Task AppendCommittedAsync(IFollowerLog log, ReplicaLogRecord[] records, CancellationToken cancellationToken)
    {
        var entries = new FollowerLogEntry[records.Length];
        for (var i = 0; i < records.Length; i++)
            entries[i] = new FollowerLogEntry(records[i].LogIndex, records[i].Term, ReplicaLogCodec.Encode(in records[i]));

        var request = new FollowerLogAppendRequest(GroupId, 1UL, 0UL, 0UL, ulong.CreateChecked(records.Length), entries);
        var appended = await log.AppendAsync(request, cancellationToken);
        if (!appended.Success)
            throw new InvalidOperationException($"The group log refused the append: {appended.RefusalCode}.");
    }

    private static IFollowerLog LogOf(ReplicaGroupRegistry registry) =>
        registry.TryGetLog(GroupId, out var log) ? log : throw new InvalidOperationException($"The group log {GroupId} is not open.");

    private static GroupIdempotencyLookup Lookup(IFollowerLog log, in ReplicaLogRecord record) =>
        log.Idempotency.Lookup(record.OperationScope, record.OperationId, record.OperationFingerprint.Span, out _);

    private static ReplicaLogRecord PrepareNoop(ulong term, ulong logIndex) =>
        Decode(new ReplicaMutationFactory(new StubCache(), GroupId, term, TimeProvider.System, NullLogger.Instance).PrepareLeaderTerm(logIndex));

    private static ReplicaLogRecord PrepareSet(string key, ulong logIndex) =>
        Decode(new ReplicaMutationFactory(new StubCache(), GroupId, 1UL, TimeProvider.System, NullLogger.Instance).PrepareSet(NewOperationId(), "cache", key, Entry(key), logIndex));

    private static ReplicaLogRecord Decode(PreparedReplicaMutation prepared) =>
        ReplicaLogCodec.Decode(prepared.CanonicalPayload) ?? ThrowHelper.Throw<ReplicaLogRecord>(new InvalidOperationException("The prepared record does not decode."));
}
