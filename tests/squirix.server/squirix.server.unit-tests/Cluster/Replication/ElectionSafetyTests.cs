using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Election safety: single vote per term, log-freshness gate, pre-vote purity, and current-term commit rule.</summary>
[Immutable]
public sealed class ElectionSafetyTests : ServerUnitTestBase
{
    private const string GroupId = "election-safety";

    /// <summary>At most one vote is granted per term and candidates with stale logs are rejected.</summary>
    [SuppressMessage("Maintainability", "SQR0005", Justification = "Test name mandated by issue #235 acceptance criteria.")]
    [Fact]
    public async Task GrantsAtMostOneVotePerTermAndRejectsStaleLog()
    {
        using var dir = new TempDirectory("squirix-election-safety-single-vote");
        await using var log = OpenLog(dir);
        await log.OpenAsync(DefaultCancellationToken);
        _ = await log.AppendAsync(Append(1UL, 1UL, "a"), DefaultCancellationToken);
        _ = await log.AppendAsync(Append(2UL, 1UL, "b"), DefaultCancellationToken);

        var first = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-a", 2UL, 2UL, 1UL), DefaultCancellationToken);
        Assert.True(first.Granted);

        var second = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 2UL, 1UL), DefaultCancellationToken);
        Assert.False(second.Granted);
        Assert.Equal(FollowerLogRefusal.AlreadyVoted, second.RefusalCode);
        Assert.Equal(2UL, second.CurrentTerm);

        var replay = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-a", 2UL, 2UL, 1UL), DefaultCancellationToken);
        Assert.True(replay.Granted);

        var staleTerm = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-c", 1UL, 2UL, 1UL), DefaultCancellationToken);
        Assert.False(staleTerm.Granted);
        Assert.Equal(FollowerLogRefusal.StaleTerm, staleTerm.RefusalCode);
        Assert.Equal(2UL, staleTerm.CurrentTerm);

        // A higher term is stepped before the freshness refusal: the term is persisted and the
        // previous vote cleared, but the stale log earns no vote.
        var staleLog = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-c", 3UL, 1UL, 1UL), DefaultCancellationToken);
        Assert.False(staleLog.Granted);
        Assert.Equal(FollowerLogRefusal.StaleLog, staleLog.RefusalCode);
        Assert.Equal(3UL, staleLog.CurrentTerm);
        var stepped = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(3UL, stepped.CurrentTerm);
        Assert.Equal(string.Empty, stepped.VotedFor);

        var current = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-c", 3UL, 2UL, 1UL), DefaultCancellationToken);
        Assert.True(current.Granted);
    }

    /// <summary>A pre-vote probe never persists or inflates the durable term.</summary>
    [SuppressMessage("Maintainability", "SQR0005", Justification = "Test name mandated by issue #235 acceptance criteria.")]
    [Fact]
    public async Task IsolatedFollowerCannotInflateTermByPreVote()
    {
        using var dir = new TempDirectory("squirix-election-safety-prevote");
        await using var log = OpenLog(dir);
        await log.OpenAsync(DefaultCancellationToken);
        var granted = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-a", 5UL, 0UL, 0UL), DefaultCancellationToken);
        Assert.True(granted.Granted);

        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await File.ReadAllBytesAsync(metaPath, DefaultCancellationToken);

        var probe = await log.TryCheckPreVoteAsync(new ElectionVoteRequest("node-b", 6UL, 0UL, 0UL), DefaultCancellationToken);
        Assert.True(probe.Granted);
        Assert.Equal(5UL, probe.CurrentTerm);

        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(5UL, status.CurrentTerm);
        Assert.Equal("node-a", status.VotedFor);
        Assert.Equal(before, await File.ReadAllBytesAsync(metaPath, DefaultCancellationToken));

        var stale = await log.TryCheckPreVoteAsync(new ElectionVoteRequest("node-c", 4UL, 0UL, 0UL), DefaultCancellationToken);
        Assert.False(stale.Granted);
        Assert.Equal(FollowerLogRefusal.StaleTerm, stale.RefusalCode);
        Assert.Equal(5UL, (await log.GetStatusAsync(DefaultCancellationToken)).CurrentTerm);
    }

    /// <summary>A zero-term request is refused on both paths and persists no vote.</summary>
    [Fact]
    public async Task ZeroTermRequestGrantsNoVote()
    {
        using var dir = new TempDirectory("squirix-election-safety-zero-term");
        await using var log = OpenLog(dir);
        await log.OpenAsync(DefaultCancellationToken);

        var metaPath = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var before = await File.ReadAllBytesAsync(metaPath, DefaultCancellationToken);

        var vote = await log.TryRequestVoteAsync(new ElectionVoteRequest("node-a", 0UL, 0UL, 0UL), DefaultCancellationToken);
        Assert.False(vote.Granted);
        Assert.Equal(FollowerLogRefusal.StaleTerm, vote.RefusalCode);

        var probe = await log.TryCheckPreVoteAsync(new ElectionVoteRequest("node-a", 0UL, 0UL, 0UL), DefaultCancellationToken);
        Assert.False(probe.Granted);
        Assert.Equal(FollowerLogRefusal.StaleTerm, probe.RefusalCode);

        var status = await log.GetStatusAsync(DefaultCancellationToken);
        Assert.Equal(0UL, status.CurrentTerm);
        Assert.Equal(string.Empty, status.VotedFor);
        Assert.Equal(before, await File.ReadAllBytesAsync(metaPath, DefaultCancellationToken));
    }

    /// <summary>An old-term majority waits: commit requires a current-term entry through the candidate index.</summary>
    [Fact]
    public void OldTermMajorityWaitsForCurrentTermEntry()
    {
        List<FollowerLogEntry> entries =
        [
            new(1UL, 1UL, new byte[] { 1 }),
            new(2UL, 1UL, new byte[] { 2 }),
        ];

        Assert.False(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 2UL, 2UL));

        entries.Add(new FollowerLogEntry(3UL, 2UL, new byte[] { 3 }));
        Assert.True(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 2UL, 3UL));
        Assert.False(ElectionCommitRule.HasCurrentTermEntryThrough(entries, 3UL, 3UL));
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
