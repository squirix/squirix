using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Election safety: single vote per term, log-freshness gate, pre-vote purity, and current-term commit rule.</summary>
[Immutable]
public sealed class ElectionSafetyTests : ServerUnitTestBase
{
    private const string GroupId = "election-safety";

    /// <summary>At most one vote is granted per term and candidates with stale logs are rejected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// #235 mandates the name "GrantsAtMostOneVotePerTermAndRejectsStaleLog"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Test]
    public async Task AtMostOneVotePerTermRejectsStaleLog(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-safety-single-vote");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), cancellationToken);

        var first = await log.RequestVoteAsync(new ElectionVoteRequest("node-a", 2UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That(first.Granted).IsTrue();

        var second = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That(second.Granted).IsFalse();
        _ = await Assert.That(second.RefusalCode).IsEqualTo(FollowerLogRefusal.AlreadyVoted);
        _ = await Assert.That(second.CurrentTerm).IsEqualTo(2UL);

        var replayMetaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var replayBefore = await File.ReadAllBytesAsync(replayMetaPath, cancellationToken);
        var replay = await log.RequestVoteAsync(new ElectionVoteRequest("node-a", 2UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That(replay.Granted).IsTrue();
        var replayBytes = await File.ReadAllBytesAsync(replayMetaPath, cancellationToken);
        await SequenceAssert.EqualAsync(replayBefore, replayBytes);

        var staleTerm = await log.RequestVoteAsync(new ElectionVoteRequest("node-c", 1UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That(staleTerm.Granted).IsFalse();
        _ = await Assert.That(staleTerm.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That(staleTerm.CurrentTerm).IsEqualTo(2UL);

        // A higher term is stepped before the freshness refusal: the term is persisted and the
        // previous vote cleared, but the stale log earns no vote.
        var staleLog = await log.RequestVoteAsync(new ElectionVoteRequest("node-c", 3UL, 1UL, 1UL), cancellationToken);
        _ = await Assert.That(staleLog.Granted).IsFalse();
        _ = await Assert.That(staleLog.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleLog);
        _ = await Assert.That(staleLog.CurrentTerm).IsEqualTo(3UL);
        var stepped = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(stepped.CurrentTerm).IsEqualTo(3UL);
        _ = await Assert.That(stepped.VotedFor).IsEqualTo(string.Empty);

        var current = await log.RequestVoteAsync(new ElectionVoteRequest("node-c", 3UL, 2UL, 1UL), cancellationToken);
        _ = await Assert.That(current.Granted).IsTrue();
    }

    /// <summary>A pre-vote probe never persists or inflates the durable term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <remarks>
    /// #235 mandates the name "IsolatedFollowerCannotInflateTermByPreVote"; it is shortened here because SQR0005
    /// limits test method names to 40 characters (mandated name documented here for traceability). Renaming a test
    /// to satisfy the analyzer changes nothing about the covered behavior.
    /// </remarks>
    [Test]
    public async Task FollowerCannotInflateTermByPreVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-safety-prevote");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        var granted = await log.RequestVoteAsync(new ElectionVoteRequest("node-a", 5UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(granted.Granted).IsTrue();

        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await File.ReadAllBytesAsync(metaPath, cancellationToken);

        var probe = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-b", 6UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(probe.Granted).IsTrue();
        _ = await Assert.That(probe.CurrentTerm).IsEqualTo(5UL);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(5UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo("node-a");
        var metaBytes = await File.ReadAllBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, metaBytes);

        var stale = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(stale.Granted).IsFalse();
        _ = await Assert.That(stale.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(5UL);
    }

    /// <summary>An old-term majority waits: commit requires a current-term entry through the candidate index.</summary>
    [Test]
    public async Task OldTermMajorityWaitsForCurrentTermEntry()
    {
        List<FollowerLogEntry> entries =
        [
            new(1UL, 1UL, new byte[] { 1 }),
            new(2UL, 1UL, new byte[] { 2 }),
        ];

        _ = await Assert.That(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 2UL, 2UL)).IsFalse();

        entries.Add(new FollowerLogEntry(3UL, 2UL, new byte[] { 3 }));
        _ = await Assert.That(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 2UL, 3UL)).IsTrue();
        _ = await Assert.That(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 3UL, 3UL)).IsFalse();
    }

    /// <summary>A zero-term request is refused on both paths and persists no vote.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ZeroTermRequestGrantsNoVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-election-safety-zero-term");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);

        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await File.ReadAllBytesAsync(metaPath, cancellationToken);

        var vote = await log.RequestVoteAsync(new ElectionVoteRequest("node-a", 0UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(vote.Granted).IsFalse();
        _ = await Assert.That(vote.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);

        var probe = await log.CheckPreVoteAsync(new ElectionVoteRequest("node-a", 0UL, 0UL, 0UL), cancellationToken);
        _ = await Assert.That(probe.Granted).IsFalse();
        _ = await Assert.That(probe.RefusalCode).IsEqualTo(FollowerLogRefusal.StaleTerm);

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(status.CurrentTerm).IsEqualTo(0UL);
        _ = await Assert.That(status.VotedFor).IsEqualTo(string.Empty);
        var metaBytes = await File.ReadAllBytesAsync(metaPath, cancellationToken);
        await SequenceAssert.EqualAsync(before, metaBytes);
    }

    /// <summary>Builds an append request for a single entry.</summary>
    /// <param name="index">The entry log index.</param>
    /// <param name="term">The entry term.</param>
    /// <param name="payload">The entry payload.</param>
    /// <returns>The append request.</returns>
    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader-1",
        term,
        index - 1,
        index == 1UL ? 0UL : term,
        0UL,
        new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes(payload))]));

    /// <summary>Opens a follower log for the election group without materializing storage yet.</summary>
    /// <param name="dir">The persistence root for the test.</param>
    /// <returns>A follower log for the election group.</returns>
    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId));
}
