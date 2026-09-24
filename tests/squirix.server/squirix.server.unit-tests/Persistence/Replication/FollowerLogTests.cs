using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Ordered durable append and rejection rules of the replica-group follower log.</summary>
[Immutable]
public sealed class FollowerLogTests : ServerUnitTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>Replaying an identical entry acknowledges idempotently without a second journal effect.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcknowledgesIdenticalDuplicateOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-duplicate");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);

        var first = await log.AppendAsync(Append(1UL, 1UL, "dup"), cancellationToken);
        var logLength = FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId));
        var second = await log.AppendAsync(Append(1UL, 1UL, "dup"), cancellationToken);

        _ = await Assert.That(first.Success).IsTrue();
        _ = await Assert.That(second.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId))).IsEqualTo(logLength);
    }

    /// <summary>A stale batch replaying already-durable entries within the local tail is still acknowledged idempotently.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcknowledgesStaleBatchWithinLocalTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-stale-repeat");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);

        var readOnlyMemory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(3UL, 1UL, Encoding.UTF8.GetBytes("c")));
        var stale = new FollowerLogAppendRequest("leader-1", 1UL, 2UL, 1UL, 0UL, readOnlyMemory);
        var result = await log.AppendAsync(stale, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
        _ = await Assert.That((await log.GetUncommittedTailAsync(cancellationToken)).Count).IsEqualTo(3);
    }

    /// <summary>The applied index advances monotonically and never beyond the committed index.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceAppliedBoundedByCommitIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-monotonic");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

        var first = await log.AdvanceAppliedAsync(1UL, cancellationToken);
        _ = await Assert.That(first.Success).IsTrue();

        var backward = await log.AdvanceAppliedAsync(0UL, cancellationToken);
        _ = await Assert.That(backward.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(1UL);

        var beyond = await log.AdvanceAppliedAsync(2UL, cancellationToken);
        _ = await Assert.That(beyond.Success).IsFalse();
        _ = await Assert.That(beyond.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(1UL);
    }

    /// <summary>Advancing the applied index releases applied entry payloads from memory.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceAppliedPrunesMemoryEntries(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-prune");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);
        _ = await log.AdvanceCommitAsync(3UL, cancellationToken);

        _ = await Assert.That((await log.GetCommittedEntriesAsync(cancellationToken)).Count).IsEqualTo(3);

        var result = await log.AdvanceAppliedAsync(2UL, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(result.AppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(2UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(3UL);
        var remaining = await log.GetCommittedEntriesAsync(cancellationToken);
        var only = await Assert.That(remaining).HasSingleItem();
        _ = await Assert.That(only.LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(Encoding.UTF8.GetString(only.Payload.Span)).IsEqualTo("c");
    }

    /// <summary>The applied watermark survives a restart and suppresses re-application of the applied prefix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdvanceAppliedSurvivesRestart(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-restart");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
            _ = await log.AdvanceCommitAsync(2UL, cancellationToken);
            var result = await log.AdvanceAppliedAsync(1UL, cancellationToken);
            _ = await Assert.That(result.Success).IsTrue();
        }

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastAppliedIndex).IsEqualTo(1UL);
            var committed = await log.GetCommittedEntriesAsync(cancellationToken);
            var singleEntry = await Assert.That(committed).HasSingleItem();
            _ = await Assert.That(singleEntry.LogIndex).IsEqualTo(2UL);
        }
    }

    /// <summary>The log takes ownership of appended payloads; mutating the caller buffer does not change stored entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendCopiesPayloadIntoOwnedBuffer(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-owned-payload");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);

        var payload = Encoding.UTF8.GetBytes("abcd");
        var request = new FollowerLogAppendRequest("leader-1", 1UL, 0UL, 0UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, payload)));
        _ = await log.AppendAsync(request, cancellationToken);

        payload[0] = 0xFF;
        payload[3] = 0xFF;

        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[0].Payload.Span)).IsEqualTo("abcd");
    }

    /// <summary>Consecutive entries become durably visible after each appending is acknowledged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendsConsecutiveEntryAfterDurableFlush(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-append");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);

        var first = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        var second = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);

        _ = await Assert.That(first.Success).IsTrue();
        _ = await Assert.That(second.Success).IsTrue();
        _ = await Assert.That(first.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(first.LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(second.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(second.LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That((await log.GetUncommittedTailAsync(cancellationToken)).Count).IsEqualTo(2);
    }

    /// <summary>
    /// After application releases the base frame's payload, a retransmission of the snapshot-base entry is
    /// acknowledged through the retained-frame term check alone: Leader Completeness forbids a conflicting
    /// term at an applied index.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliedBaseDuplicateAcceptedByTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-base-duplicate-applied");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(1UL, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceAppliedAsync(1UL, cancellationToken)).Success).IsTrue();

        // CreateSnapshotAsync installs the baseline without compacting the journal, so the released frame
        // keeps its offset in EntryOffsets and a retransmission must be an exact duplicate, not a conflict.
        var snapshot = await log.CreateSnapshotAsync(1UL, cancellationToken);
        _ = await Assert.That(snapshot.LastIncludedIndex).IsEqualTo(1UL);

        var retransmission = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);

        _ = await Assert.That(retransmission.Success).IsTrue();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>A batch entry claiming a conflicting term at an applied index fails readiness instead of being silently accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliedConflictInBatchFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-conflict-batch");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

        // The batch rewrites the applied entry at index 1 with a conflicting term and appends a successor.
        var readOnlyMemory = ReadOnlyMemory<FollowerLogEntry>.Of(
            new FollowerLogEntry(1UL, 2UL, Encoding.UTF8.GetBytes("x")),
            new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("y")));
        var request = new FollowerLogAppendRequest("leader-2", 2UL, 0UL, 0UL, 0UL, readOnlyMemory);
        var result = await log.AppendAsync(request, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>A previous-log term conflict at an applied index fails readiness instead of being silently accepted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppliedPreviousLogConflictFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-prev-conflict");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

        // The leader claims its previous entry at the applied index 1 has a conflicting term.
        var readOnlyMemory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("b")));
        var request = new FollowerLogAppendRequest("leader-2", 2UL, 1UL, 2UL, 0UL, readOnlyMemory);
        var result = await log.AppendAsync(request, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>A batch whose first entry does not follow the declared predecessor is rejected as malformed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task BatchMustFollowDeclaredPredecessor(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-predecessor");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AppendAsync(Append(3UL, 1UL, "c"), cancellationToken);

        // Declares index 1 as predecessor but skips index 2: the batch starts at index 3.
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(3UL, 1UL, Encoding.UTF8.GetBytes("c")), new FollowerLogEntry(4UL, 1UL, Encoding.UTF8.GetBytes("d")));
        var malformed = new FollowerLogAppendRequest("leader-1", 1UL, 1UL, 1UL, 0UL, memory);
        var result = await log.AppendAsync(malformed, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    /// <summary>The commit index never moves backward even when a lower request arrives.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommitIndexNeverMovesBackward(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-commit-backward");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

        var back = await log.AdvanceCommitAsync(0UL, cancellationToken);
        _ = await Assert.That(back.Success).IsTrue();
        _ = await Assert.That(back.CommitIndex).IsEqualTo(1UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(1UL);
    }

    /// <summary>A term conflict at the committed boundary also fails readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedBoundaryConflictFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-committed-boundary");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

        // The leader's previous entry at the committed index disagrees in term.
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 3UL, Encoding.UTF8.GetBytes("b")));
        var request = new FollowerLogAppendRequest("leader-2", 3UL, 1UL, 2UL, 0UL, memory);
        var result = await log.AppendAsync(request, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A conflict at or below the committed index fails readiness without truncating anything.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CommittedConflictFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-committed-conflict");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        var result = await log.AppendAsync(Append(1UL, 2UL, "x"), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>A commit index beyond the durable last index is refused.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DoesNotCommitBeyondDurableLastIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-commit-beyond");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);

        var result = await log.AdvanceCommitAsync(9UL, cancellationToken);
        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(result.CommitIndex).IsEqualTo(0UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(0UL);
    }

    /// <summary>
    /// A leader retransmission of the committed base frame passes the exact payload comparison while the frame
    /// is still retained in memory, and stays acknowledged through application and baseline installation.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DuplicateAppendAtSnapshotBaseAccepted(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-base-duplicate");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(1UL, cancellationToken)).Success).IsTrue();

        // While index one is still retained in Entries, the identical retransmission must pass the exact
        // payload comparison rather than the applied-region term-only acceptance.
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();

        _ = await Assert.That((await log.AdvanceAppliedAsync(1UL, cancellationToken)).Success).IsTrue();
        var snapshot = await log.CreateSnapshotAsync(1UL, cancellationToken);
        _ = await Assert.That(snapshot.LastIncludedIndex).IsEqualTo(1UL);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>A duplicate-prefix request cannot commit entries beyond the prefix it validates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DuplicatePrefixBlocksUnvalidatedTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-duplicate-prefix");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(3UL, 2UL, "c", 1UL), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(4UL, 2UL, "d", 2UL), cancellationToken)).Success).IsTrue();
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        // The leader re-sends only entry 3 and claims index 4 committed; entry 4 was not validated by this request.
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("c")));
        var duplicate = new FollowerLogAppendRequest("leader-3", 3UL, 2UL, 1UL, 4UL, memory);
        var result = await log.AppendAsync(duplicate, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(3UL);
        _ = await Assert.That((await log.GetCommittedEntriesAsync(cancellationToken)).Count).IsEqualTo(3);
    }

    /// <summary>A crafted group identifier cannot escape the storage root because segments are hex-encoded.</summary>
    [Test]
    public async Task GroupIdCannotEscapeStorageRoot()
    {
        using var dir = new TempDirectory("squirix-follower-log-escape");
        const string evil = @"..\..\escape";

        var segment = GroupStoragePaths.EncodeGroupSegment(evil);
        _ = await Assert.That(segment).DoesNotContain("..", StringComparison.Ordinal);

        var path = GroupStoragePaths.GetGroupDirectory(dir, evil);
        _ = await Assert.That(path).StartsWith(dir, StringComparison.Ordinal);
    }

    /// <summary>A frame body declared larger than the maximum bound is rejected during core validation.</summary>
    [Test]
    public async Task GroupLogRejectsCoreBodyTooLarge()
    {
        var result = CheckBodyTooLarge();
        _ = await Assert.That(result).IsFalse();
        return;

        static bool CheckBodyTooLarge()
        {
            Span<byte> buffer = [0x53, 0x51, 0x52, 0x4C, 0x01, 0xFF, 0xFF, 0xFF, 0x7F];
            return GroupLogCodec.TryReadFrame(buffer, out _);
        }
    }

    /// <summary>A frame body declared shorter than the fixed fields is rejected during core validation.</summary>
    [Test]
    public async Task GroupLogRejectsCoreBodyTooShort()
    {
        var result = CheckBodyTooShort();
        _ = await Assert.That(result).IsFalse();
        return;

        static bool CheckBodyTooShort()
        {
            Span<byte> buffer = [0x53, 0x51, 0x52, 0x4C, 0x01, 0x0A, 0x00, 0x00, 0x00];
            return GroupLogCodec.TryReadFrame(buffer, out _);
        }
    }

    /// <summary>A frame whose declared body cannot fit the available buffer is rejected during core validation.</summary>
    [Test]
    public async Task GroupLogRejectsCoreBodyTruncated()
    {
        var result = CheckBodyTruncated();
        _ = await Assert.That(result).IsFalse();
        return;

        static bool CheckBodyTruncated()
        {
            Span<byte> buffer = stackalloc byte[15];
            buffer[0] = 0x53;
            buffer[1] = 0x51;
            buffer[2] = 0x52;
            buffer[3] = 0x4C;
            buffer[4] = 0x01;
            BinaryPrimitives.WriteInt32LittleEndian(buffer[5..], 20);
            return GroupLogCodec.TryReadFrame(buffer, out _);
        }
    }

    /// <summary>A frame header declaring a body shorter than the fixed fields is rejected without allocating.</summary>
    [Test]
    public async Task GroupLogRejectsHeaderBodyTooShort()
    {
        var result = CheckHeaderTooShort();
        _ = await Assert.That(result).IsFalse();
        return;

        static bool CheckHeaderTooShort()
        {
            Span<byte> header = [0x53, 0x51, 0x52, 0x4C, 0x01, 0x0A, 0x00, 0x00, 0x00];
            return GroupLogCodec.TryReadFrameHeaderLength(header, out _);
        }
    }

    /// <summary>An oversized payload is rejected while sizing the encoded buffer, before any pool rent.</summary>
    [Test]
    public void GroupLogRejectsOversizedPayloadLength() =>
        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(static () => GroupLogCodec.ComputeFrameEncodedLength(int.MaxValue));

    /// <summary>A heartbeat with a high commit index does not commit a retained divergent suffix beyond the verified predecessor.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HeartbeatIgnoresDivergentSuffix(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-heartbeat-divergent");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(3UL, 2UL, "c", 1UL), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(4UL, 2UL, "d", 2UL), cancellationToken)).Success).IsTrue();
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        // A new term-3 leader heartbeat at index 2 and claims index 4 committed; the term-2 suffix stays uncommitted.
        var heartbeat = new FollowerLogAppendRequest("leader-3", 3UL, 2UL, 1UL, 4UL, ReadOnlyMemory<FollowerLogEntry>.Empty);
        var result = await log.AppendAsync(heartbeat, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(2UL);
        _ = await Assert.That((await log.GetCommittedEntriesAsync(cancellationToken)).Count).IsEqualTo(2);
    }

    /// <summary>
    /// A commit request from a higher-term leader adopts the term durably even when the commit index is monotonic,
    /// and subsequent delayed requests are evaluated against the persisted term.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermCommitAdoptsTermOnNoOp(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-higher-term-commit");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

            // A higher-term leader sends a commit already satisfied monotonically; the term must still be adopted.
            var noOp = await log.AdvanceCommitAsync(1UL, 2UL, cancellationToken);
            _ = await Assert.That(noOp.Success).IsTrue();

            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That(status.CurrentTerm).IsEqualTo(2UL);
            _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);

            // A delayed request from the deposed lower-term leader is now rejected against the persisted term.
            var stale = await log.AdvanceCommitAsync(1UL, 1UL, cancellationToken);
            _ = await Assert.That(stale.Success).IsFalse();
            _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        }

        // The adopted term survives restart and still governs stale-term rejections.
        await using (var reopened = new FollowerLog(dir, GroupId, composition))
        {
            await reopened.OpenAsync(cancellationToken);
            _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(2UL);

            var stale = await reopened.AdvanceCommitAsync(1UL, 1UL, cancellationToken);
            _ = await Assert.That(stale.Success).IsFalse();
            _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        }
    }

    /// <summary>
    /// A compacted snapshot base is refused because the probe lies below the snapshot boundary: the frame was
    /// compacted away and its term is unverifiable, so the below-boundary rule (not a payload comparison) produces
    /// the LogMismatch refusal while Readiness stays Ready.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ProbeBelowCompactedBoundaryRejected(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-compacted-snapshot-base-conflict");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceAppliedAsync(2UL, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.CreateSnapshotAsync(2UL, cancellationToken)).LastIncludedIndex).IsEqualTo(2UL);
        var compact = await log.CompactAsync(cancellationToken);
        _ = await Assert.That(compact.Success).IsTrue();

        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("conflict")));
        var request = new FollowerLogAppendRequest("leader-1", 1UL, 1UL, 1UL, 0UL, memory);
        var result = await log.AppendAsync(request, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>Re-appending an already-applied entry is acknowledged idempotently without failing readiness.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReappliedEntryAcknowledgedOnce(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-applied-reapply");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

        var result = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(await log.GetCommittedEntriesAsync(cancellationToken)).IsEmpty();
    }

    /// <summary>A gap in the batch is rejected without any appending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsGapWithoutAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-reject");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "one"), cancellationToken);

        var gap = await log.AppendAsync(Append(3UL, 1UL, "three"), cancellationToken);
        _ = await Assert.That(gap.Success).IsFalse();
        _ = await Assert.That(gap.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>An oversized frame body length in the committed group log is rejected during recovery instead of renting a multi-GB buffer.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsOversizedFrameBodyLength(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-oversized");
        var composition = GroupComposition.Create(GroupId);

        await using (var log = new FollowerLog(dir, GroupId, composition))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
        }

        var path = GroupStoragePaths.GetLogPath(dir, GroupId);
        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);

        // Frame header follows the 5-byte file header: magic(4) | version(1) | bodyLength(4). Overwrite bodyLength with a multi-GB value.
        const int bodyLengthOffset = 5 + 4 + 1;
        bytes[bodyLengthOffset] = 0xFF;
        bytes[bodyLengthOffset + 1] = 0xFF;
        bytes[bodyLengthOffset + 2] = 0xFF;
        bytes[bodyLengthOffset + 3] = 0x7F;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        await using var reopened = new FollowerLog(dir, GroupId, composition);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(reopened.OpenAsync(cancellationToken));
        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>An appending carrying a lower term than the durable term is rejected before any mutation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsStaleTermBeforeAppend(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-stale-term");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 5UL, "x"), cancellationToken);

        var stale = await log.AppendAsync(Append(2UL, 4UL, "y"), cancellationToken);
        _ = await Assert.That(stale.Success).IsFalse();
        _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// A re-sent snapshot base entry with a conflicting payload is rejected while the base entry is still
    /// retained in memory, even though the index and term match the snapshot baseline.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ResidentBasePayloadConflictRejected(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-snapshot-base-conflict");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.AdvanceCommitAsync(2UL, cancellationToken)).Success).IsTrue();
        _ = await Assert.That((await log.CreateSnapshotAsync(2UL, cancellationToken)).LastIncludedIndex).IsEqualTo(2UL);

        // The leader re-sends the snapshot base entry at index 2 with the same term but a different payload while the
        // local basis entry is still resident; the exact payload comparison must reject the conflict instead of the
        // snapshot-baseline term fallback acknowledging it.
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 1UL, Encoding.UTF8.GetBytes("conflict")));
        var request = new FollowerLogAppendRequest("leader-1", 1UL, 1UL, 1UL, 0UL, memory);
        var result = await log.AppendAsync(request, cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
    }

    /// <summary>Status exposes the durable group identity and metadata along with the journal watermarks.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StatusExposesDurableGroupMetadata(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-status");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);

        var status = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That(status.GroupId).IsEqualTo(GroupId);
        _ = await Assert.That(status.TopologyFingerprint.IsEmpty).IsTrue();
        _ = await Assert.That(status.ConfigurationGeneration).IsEqualTo(0UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(1UL);
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(1UL);
        _ = await Assert.That(status.CommitIndex).IsEqualTo(0UL);
        _ = await Assert.That(status.LastAppliedIndex).IsEqualTo(0UL);
        _ = await Assert.That(status.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
    }

    /// <summary>An uncommitted entry conflicting with the leader's batch is truncated and rewritten.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatesConflictingUncommittedTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-conflict");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "old"), cancellationToken);

        var result = await log.AppendAsync(Append(1UL, 2UL, "new"), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].Term).IsEqualTo(2UL);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[0].Payload.Span)).IsEqualTo("new");
    }

    /// <summary>A conflict in the middle of the log truncates the divergent tail and rewrites it with the leader's entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatesMidLogConflictAndRewritesTail(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-truncate-mid");
        var composition = GroupComposition.Create(GroupId);

        await using var log = new FollowerLog(dir, GroupId, composition);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);
        _ = await log.AdvanceCommitAsync(1UL, cancellationToken);

        // New leader (term 2) rewrites index 2 (which conflicts) and appends index 3.
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("B")), new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("C")));
        var batch = new FollowerLogAppendRequest("leader-2", 2UL, 1UL, 1UL, 0UL, memory);
        var result = await log.AppendAsync(batch, cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail.Count).IsEqualTo(2);
        _ = await Assert.That(tail[0].LogIndex).IsEqualTo(2UL);
        _ = await Assert.That(tail[0].Term).IsEqualTo(2UL);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[0].Payload.Span)).IsEqualTo("B");
        _ = await Assert.That(tail[1].LogIndex).IsEqualTo(3UL);
        _ = await Assert.That(tail[1].Term).IsEqualTo(2UL);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[1].Payload.Span)).IsEqualTo("C");
    }

    /// <summary>Opening a group outside the local static composition does not create any storage.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnknownGroupCreatesNoDirectory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-log-unknown-group");
        var composition = GroupComposition.Empty();

        await using var log = new FollowerLog(dir, "unknown", composition);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(log.OpenAsync(cancellationToken));

        var root = GroupStoragePaths.GetRoot(dir);
        _ = await Assert.That(Directory.Exists(root)).IsFalse();
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload, ulong? prevLogTerm = null)
    {
        var memory = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload)));
        return new FollowerLogAppendRequest("leader-1", term, index - 1, prevLogTerm ?? (index == 1UL ? 0UL : term), 0UL, memory);
    }
}
