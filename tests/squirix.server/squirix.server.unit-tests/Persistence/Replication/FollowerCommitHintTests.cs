using System;
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

/// <summary>The commit index a follower learns from an append is held in memory and made durable by the apply loop, never below a committed entry.</summary>
[Immutable]
public sealed class FollowerCommitHintTests : ServerUnitTestBase
{
    private const string GroupId = "grp-hint";
    private const string LeaderId = "leader-1";
    private const ulong Term = 1UL;

    /// <summary>An append that announces a commit index moves the in-memory hint only: the durable commit index and the metadata file stay as they were.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AppendAnnouncesCommitInMemory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-announce");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);

        _ = await Assert.That((await log.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken)).Success).IsTrue();

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitHint, status.CommitIndex, status.EffectiveCommitIndex)).IsEqualTo((2UL, 0UL, 2UL));
        _ = await Assert.That(GroupMetadataStore.Read(GroupStoragePaths.GetMetadataPath(dir, GroupId)).Metadata.CommitIndex).IsEqualTo(0UL);
    }

    /// <summary>The hint never exceeds the last index the append verified against the leader log, whatever commit index the leader announces.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HintIsLimitedToVerifiedIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-verified");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);

        _ = await log.AppendAsync(Heartbeat(2UL, 9UL), cancellationToken);

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitHint).IsEqualTo(2UL);
    }

    /// <summary>A later append that announces a lower commit index leaves the hint where it was.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HintNeverMovesBack(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-monotonic");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);
        _ = await log.AppendAsync(Heartbeat(3UL, 3UL), cancellationToken);

        _ = await log.AppendAsync(Heartbeat(3UL, 1UL), cancellationToken);

        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CommitHint).IsEqualTo(3UL);
    }

    /// <summary>Making the hint durable publishes it once; a restart without that forgets it and learns it again from the leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OnlyAdvanceMakesTheHintDurable(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-durable");
        await using (var log = await OpenWithEntriesAsync(dir, 3, cancellationToken))
            _ = await log.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken);

        await using (var forgotten = Open(dir))
        {
            await forgotten.OpenAsync(cancellationToken);
            var status = await forgotten.GetStatusAsync(cancellationToken);
            _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.CommitHint)).IsEqualTo((3UL, 0UL, 0UL));
            _ = await forgotten.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken);
            _ = await forgotten.AdvanceCommitAsync(2UL, cancellationToken);
        }

        await using var durable = Open(dir);
        await durable.OpenAsync(cancellationToken);
        _ = await Assert.That((await durable.GetStatusAsync(cancellationToken)).CommitIndex).IsEqualTo(2UL);
    }

    /// <summary>An entry of a different term at an index the hint covers is a committed conflict: readiness fails and nothing is truncated.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConflictBelowHintFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-conflict");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);
        _ = await log.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken);
        var replacement = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(2UL, 2UL, Encoding.UTF8.GetBytes("x")));

        var result = await log.AppendAsync(new FollowerLogAppendRequest("leader-2", 2UL, 1UL, Term, 0UL, replacement), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(3UL);
    }

    /// <summary>A leader whose previous entry disagrees in term at an index the hint covers violates leader completeness: readiness fails.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PreviousTermBelowHintFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-previous");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);
        _ = await log.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken);

        var result = await log.AppendAsync(new FollowerLogAppendRequest("leader-2", 2UL, 2UL, 2UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
    }

    /// <summary>A conflict above the hint is truncated and replaced as before: only a committed entry is protected.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConflictAboveHintIsReplaced(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-above");
        await using var log = await OpenWithEntriesAsync(dir, 3, cancellationToken);
        _ = await log.AppendAsync(Heartbeat(3UL, 2UL), cancellationToken);
        var replacement = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(3UL, 2UL, Encoding.UTF8.GetBytes("x")));

        var result = await log.AppendAsync(new FollowerLogAppendRequest("leader-2", 2UL, 2UL, Term, 2UL, replacement), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(await log.GetTermAtAsync(3UL, cancellationToken)).IsEqualTo(2UL);
    }

    /// <summary>A state transfer at an index below the hint is refused as not ready, although the durable commit index would allow it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TransferBelowHintIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-transfer");
        await using var log = StateInstallScenario.NewLog(dir);
        await StateInstallScenario.OpenBehindAsync(log, cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 3UL, 1UL, "c"), cancellationToken);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 4UL, 1UL, "d"), cancellationToken);
        _ = await log.AppendAsync(new FollowerLogAppendRequest("leader", 1UL, 4UL, 1UL, 4UL, ReadOnlyMemory<FollowerLogEntry>.Empty), cancellationToken);

        var refusal = await FollowerSnapshotScenario.BeginAsync(log, 3UL, 1UL, cancellationToken);

        _ = await Assert.That(refusal).IsEqualTo(FollowerLogRefusal.NotReady);
        _ = await Assert.That(log.IsComplete).IsTrue();
    }

    /// <summary>A compaction of the applied prefix leaves the hint, which says nothing about the entries it removes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactionKeepsTheHint(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-hint-compaction");
        await using var log = await OpenWithEntriesAsync(dir, 4, cancellationToken);
        _ = await log.AppendAsync(Heartbeat(4UL, 4UL), cancellationToken);
        _ = await log.AdvanceCommitAsync(2UL, cancellationToken);

        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(log, dir, 2UL, cancellationToken);

        _ = await Assert.That(snapshot.LastIncludedIndex).IsEqualTo(2UL);
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CommitHint, status.CommitIndex)).IsEqualTo((4UL, 2UL));
    }

    private static FollowerLogAppendRequest Heartbeat(ulong prevLogIndex, ulong leaderCommitIndex) =>
        new(LeaderId, Term, prevLogIndex, Term, leaderCommitIndex, ReadOnlyMemory<FollowerLogEntry>.Empty);

    private static FollowerLog Open(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), Microsoft.Extensions.Logging.Abstractions.NullLogger<FollowerLog>.Instance, TimeProvider.System);

    private static async Task<FollowerLog> OpenWithEntriesAsync(TempDirectory dir, int count, CancellationToken cancellationToken)
    {
        var log = Open(dir);
        try
        {
            await log.OpenSeededAsync(cancellationToken);
            for (var i = 1; i <= count; i++)
            {
                var index = ulong.CreateChecked(i);
                var entry = ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, Term, Encoding.UTF8.GetBytes("e" + i.ToString(System.Globalization.CultureInfo.InvariantCulture))));
                var appended = await log.AppendAsync(new FollowerLogAppendRequest(LeaderId, Term, index - 1UL, index == 1UL ? 0UL : Term, 0UL, entry), cancellationToken);
                if (!appended.Success)
                    throw new InvalidOperationException($"The log refused entry {i}: {appended.RefusalCode}.");
            }

            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
