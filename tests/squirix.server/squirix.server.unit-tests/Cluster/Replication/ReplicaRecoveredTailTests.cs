using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Recovered-tail commit checks preserve the current-term boundary without rescanning entries.</summary>
[Immutable]
public sealed class ReplicaRecoveredTailTests : ServerUnitTestBase
{
    /// <summary>The earliest current-term entry permits its index and later indexes.</summary>
    /// <param name="index">The candidate index.</param>
    /// <param name="expected">Whether the candidate passes the recovered-tail rule.</param>
    [Test]
    [Arguments(9UL, false)]
    [Arguments(10UL, false)]
    [Arguments(11UL, true)]
    [Arguments(12UL, true)]
    [Arguments(13UL, true)]
    [Arguments(14UL, true)]
    public async Task CommitUsesFirstCurrentTermIndex(ulong index, bool expected)
    {
        var tail = new ReplicaRecoveredTail(
            [Entry(10UL, 2UL), Entry(11UL, 3UL), Entry(12UL, 2UL), Entry(13UL, 3UL)],
            3UL,
            new TailRebuilder());

        _ = await Assert.That(tail.CommitBoundary.CanCommitThrough(index)).IsEqualTo(expected);
    }

    /// <summary>Without a current-term entry only candidates beyond the recovered tail pass.</summary>
    /// <param name="index">The candidate index.</param>
    /// <param name="expected">Whether the candidate passes the recovered-tail rule.</param>
    [Test]
    [Arguments(0UL, false)]
    [Arguments(10UL, false)]
    [Arguments(11UL, false)]
    [Arguments(12UL, true)]
    public async Task CommitWithoutCurrentTermNeedsLaterIndex(ulong index, bool expected)
    {
        var tail = new ReplicaRecoveredTail([Entry(10UL, 2UL), Entry(11UL, 2UL)], 3UL, new TailRebuilder());

        _ = await Assert.That(tail.CommitBoundary.CanCommitThrough(index)).IsEqualTo(expected);
    }

    /// <summary>The maximum index is a valid boundary, distinct from an absent current-term entry.</summary>
    /// <param name="currentTerm">The current leader term.</param>
    /// <param name="expected">Whether the maximum index passes the recovered-tail rule.</param>
    [Test]
    [Arguments(2UL, true)]
    [Arguments(3UL, false)]
    public async Task MaximumIndexIsNotAnAbsentBoundary(ulong currentTerm, bool expected)
    {
        var tail = new ReplicaRecoveredTail([Entry(ulong.MaxValue, 2UL)], currentTerm, new TailRebuilder());

        _ = await Assert.That(tail.CommitBoundary.CanCommitThrough(ulong.MaxValue - 1UL)).IsFalse();
        _ = await Assert.That(tail.CommitBoundary.CanCommitThrough(ulong.MaxValue)).IsEqualTo(expected);
    }

    /// <summary>Repeated checks read only the last index and never scan the recovered entries again.</summary>
    [Test]
    public async Task CommitChecksDoNotRescanEntries()
    {
        var entries = new CountingEntries([Entry(10UL, 2UL), Entry(11UL, 2UL), Entry(12UL, 3UL)]);
        var tail = new ReplicaRecoveredTail(entries, 3UL, new TailRebuilder());
        var readsBefore = entries.IndexReads;
        var beforeBoundary = tail.CommitBoundary.CanCommitThrough(11UL);
        var atBoundary = tail.CommitBoundary.CanCommitThrough(12UL);
        var reads = entries.IndexReads - readsBefore;

        _ = await Assert.That(beforeBoundary).IsFalse();
        _ = await Assert.That(atBoundary).IsTrue();
        _ = await Assert.That(reads).IsEqualTo(2);
    }

    /// <summary>The coordinator retains prepared entries, not the recovered container or its original payload buffers.</summary>
    [Test]
    [SuppressMessage("Major Code Smell", "S1215", Justification = "Collection verifies that recovered buffers are no longer rooted.")]
    public async Task CoordinatorReleasesRecoveredTail()
    {
        var retained = CreateRetentionCase();
        await using var coordinator = retained.Coordinator;

        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

        _ = await Assert.That(retained.Tail.IsAlive).IsFalse();
        _ = await Assert.That(retained.Source.IsAlive).IsFalse();
        _ = await Assert.That(retained.Mutation.IsAlive).IsTrue();
        _ = await Assert.That(retained.Canonical.IsAlive).IsTrue();
        _ = await Assert.That(coordinator.HasPendingApply).IsTrue();
        GC.KeepAlive(coordinator);
    }

    /// <summary>Applying a recovered entry releases its prepared payload even while its coordinator remains usable.</summary>
    [Test]
    [SuppressMessage("Major Code Smell", "S1215", Justification = "Collection verifies that applied buffers are no longer rooted.")]
    public async Task AppliedTailReleasesPayload()
    {
        var retained = CreateRetentionCase();
        await using var coordinator = retained.Coordinator;

        // Apply inline to exclude transient references on foreign-apply worker stacks from this retention check.
        await coordinator.PendingApply.ApplyThroughAsync(1, 1, 1, false);

        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, true, true);

        _ = await Assert.That(retained.Mutation.IsAlive).IsFalse();
        _ = await Assert.That(retained.Canonical.IsAlive).IsFalse();
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
        _ = await Assert.That(retained.Pipeline.Applied.Count).IsEqualTo(1);
        _ = await Assert.That(retained.Pipeline.Applied.TryPeek(out var applied)).IsTrue();
        _ = await Assert.That(applied).IsEqualTo(1UL);
        GC.KeepAlive(coordinator);
    }

    [SuppressMessage("Reliability", "CA2000", Justification = "Ownership is transferred to the test, which asynchronously disposes the coordinator.")]
    [MethodImpl(MethodImplOptions.NoInlining)]
    private static TailRetentionCase CreateRetentionCase()
    {
        var source = new byte[1024 * 1024];
        var tail = new ReplicaRecoveredTail([new FollowerLogEntry(1, 1, source)], 1, new TailRebuilder());
        var mutation = tail.Mutations[0];
        _ = MemoryMarshal.TryGetArray(mutation.CanonicalPayload, out var canonical);
        var pipeline = new RunPipeline();
        var coordinator = new ReplicaCommitCoordinator(
            new ReplicaCommitCoordinatorOptions(2, 1, 0, 4),
            pipeline,
            ReplicaFaultHooks.CreateNoOp(),
            new GroupIdempotencyState(10, TimeSpan.MaxValue, TimeProvider.System),
            null,
            tail)
        { ObserveTimeProvider = TimeProvider.System, BudgetTimeProvider = TimeProvider.System };
        return new TailRetentionCase(coordinator, pipeline, new WeakReference(tail), new WeakReference(source), new WeakReference(mutation), new WeakReference(canonical.Array));
    }

    private static FollowerLogEntry Entry(ulong index, ulong term) => new(index, term, ReadOnlyMemory<byte>.Empty);

    [Immutable]
    private sealed record TailRetentionCase(
        ReplicaCommitCoordinator Coordinator,
        RunPipeline Pipeline,
        WeakReference Tail,
        WeakReference Source,
        WeakReference Mutation,
        WeakReference Canonical);

    [Mutable]
    private sealed class CountingEntries : IReadOnlyList<FollowerLogEntry>
    {
        private readonly IReadOnlyList<FollowerLogEntry> _entries;

        internal CountingEntries(IReadOnlyList<FollowerLogEntry> entries)
        {
            _entries = entries;
        }

        public int Count => _entries.Count;

        public int IndexReads { get; private set; }

        public FollowerLogEntry this[int index]
        {
            get
            {
                IndexReads++;
                return _entries[index];
            }
        }

        public IEnumerator<FollowerLogEntry> GetEnumerator() => _entries.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
    }

    [Immutable]
    private sealed class TailRebuilder : IReplicaTailRebuilder
    {
        public PreparedReplicaMutation Rebuild(FollowerLogEntry entry) => new(
            new ReplicaOperationIdentity("group-a", "client", entry.LogIndex.ToString("x32", CultureInfo.InvariantCulture), new byte[] { 1 }),
            entry.Term,
            entry.LogIndex,
            new ReplicaMutationPayload(entry.Payload, new byte[] { 3 }, 4));
    }
}
