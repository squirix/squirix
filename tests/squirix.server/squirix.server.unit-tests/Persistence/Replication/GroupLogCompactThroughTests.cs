using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Gated compaction of a replica group log through its committed and applied index.</summary>
public sealed class GroupLogCompactThroughTests : ServerUnitTestBase
{
    private const ulong Committed = 3UL;
    private const int EntryCount = 3;
    private const string GroupId = "grp-compact-through";

    /// <summary>A durability boundary of the compaction.</summary>
    public enum FaultBoundary
    {
        /// <summary>The snapshot is published; the rewritten log is flushed to its temp file but not yet swapped in.</summary>
        AfterSnapshotPublish = 0,

        /// <summary>The log is swapped for the header-only rewrite; the metadata is written to its temp file but not yet published.</summary>
        AfterLogReplace = 1,
    }

    /// <summary>A committed and applied log compacts to its header alone, keeps its last term from the baseline, and reopens the same.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompactsToHeaderOnly(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through");
        await using (var log = await SeedAsync(dir, null, cancellationToken))
        {
            _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);

            var retention = await RetentionAsync(log, cancellationToken);
            _ = await Assert.That(retention).IsEqualTo(new FollowerLogRetention(HeaderLength(), 0, 0, Committed));
            await AssertCompactedStateAsync(log, cancellationToken);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);
        await AssertCompactedStateAsync(reopened, cancellationToken);
    }

    /// <summary>A range read below the snapshot baseline is not retained, while one starting right above it verifies against the baseline term.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadBelowSnapshotBaselineIsNotRetained(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-read");
        await using var log = await SeedAsync(dir, null, cancellationToken);
        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", Committed + 1, 1UL, "after"), cancellationToken);

        var compacted = await log.ReadEntriesAsync(Committed, 1, cancellationToken);
        var aboveBaseline = await log.ReadEntriesAsync(Committed + 1, 1, cancellationToken);

        _ = await Assert.That((compacted.Retained, compacted.PrevLogIndex, compacted.PrevLogTerm)).IsEqualTo((false, Committed - 1, 0UL));
        _ = await Assert.That(compacted.Entries).IsEmpty();
        _ = await Assert.That((aboveBaseline.Retained, aboveBaseline.PrevLogIndex, aboveBaseline.PrevLogTerm, aboveBaseline.LastLogIndex, aboveBaseline.CommitIndex))
                          .IsEqualTo((true, Committed, 1UL, Committed + 1, Committed));
        _ = await Assert.That(aboveBaseline.Entries.Count).IsEqualTo(1);
        _ = await Assert.That((aboveBaseline.Entries[0].LogIndex, Encoding.UTF8.GetString(aboveBaseline.Entries[0].Payload.Span))).IsEqualTo((Committed + 1, "after"));
    }

    /// <summary>An entry at or below the index with an unresolved outcome refuses the compaction before anything changes, readiness included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesUnresolvedOutcome(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-unresolved");
        await using var log = await SeedAsync(dir, null, cancellationToken);
        _ = log.Idempotency.Reserve("client", "in-flight", [9], GroupRecordKind.UserMutation, 2UL, 1UL);

        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.UnresolvedOutcome);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>An index that is not both the commit and the applied index is refused before anything changes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesUnappliedIndex(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-unapplied");
        await using var log = await SeedAsync(dir, null, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Committed - 1, cancellationToken)).IsEqualTo(GroupCompactionOutcome.NotReady);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>A snapshot past the configured maximum size is refused before anything is written, readiness included.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusesOversizedSnapshot(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-oversized");
        await using var log = await SeedAsync(dir, new FollowerLogOptions { MaxSnapshotBytes = 64 }, cancellationToken);

        _ = await Assert.That(await log.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.SnapshotTooLarge);

        await AssertUnchangedAsync(log, cancellationToken);
    }

    /// <summary>A failure at either durability boundary of the compaction leaves a log that reopens to the committed, applied state.</summary>
    /// <param name="boundary">The boundary the failure is injected at.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(FaultBoundary.AfterSnapshotPublish)]
    [Arguments(FaultBoundary.AfterLogReplace)]
    public async Task FaultReopensConsistent(FaultBoundary boundary, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-compact-through-fault");
        var armed = false;
        var hooks = new IFollowerLogFaultHooksCreateExpectations();
        _ = hooks.Setups.OnFrameWritten();
        _ = hooks.Setups.OnCommitAdvanced();
        _ = hooks.Setups.OnBeforeMemoryApply();
        _ = hooks.Setups.OnFlushed().Callback(() => FailIfArmed(armed && boundary == FaultBoundary.AfterSnapshotPublish));
        _ = hooks.Setups.OnMetaWritten().Callback(() => FailIfArmed(armed && boundary == FaultBoundary.AfterLogReplace));
        await using (var log = await SeedAsync(dir, new FollowerLogOptions { FaultHooks = hooks.Instance() }, cancellationToken))
        {
            armed = true;
            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.CompactThroughAsync(Committed, cancellationToken));
            armed = false;
        }

        // The failure hit the intended boundary: the snapshot is published in both cases, and only the later one swapped the log.
        var logBytes = new FileInfo(GroupStoragePaths.GetLogPath(dir, GroupId)).Length;
        _ = await Assert.That(File.Exists(GroupStoragePaths.GetSnapshotPath(dir, GroupId))).IsTrue();
        _ = await Assert.That(logBytes == HeaderLength()).IsEqualTo(boundary == FaultBoundary.AfterLogReplace);

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);
        await reopened.OpenAsync(cancellationToken);

        _ = await Assert.That(reopened.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.LastLogIndex, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((Committed, Committed, Committed));
        _ = await Assert.That(await reopened.GetCommittedEntriesAsync(cancellationToken)).IsEmpty();
        await AssertOutcomesAsync(reopened);
        _ = await Assert.That(await reopened.CompactThroughAsync(Committed, cancellationToken)).IsEqualTo(GroupCompactionOutcome.Compacted);
        await AssertCompactedStateAsync(reopened, cancellationToken);
    }

    private static void FailIfArmed(bool armed)
    {
        if (armed)
            throw new IOException("simulated crash at a compaction boundary.");
    }

    private static long HeaderLength() => GroupLogCodec.LogFileHeader.Length;

    private static async Task AssertCompactedStateAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That((status.LastLogIndex, status.LastLogTerm, status.CommitIndex, status.LastAppliedIndex)).IsEqualTo((Committed, 1UL, Committed, Committed));
        _ = await Assert.That(await log.GetTermAtAsync(Committed, cancellationToken)).IsEqualTo(1UL);
        _ = await Assert.That(log.SnapshotPath).IsNotNull();
        await AssertOutcomesAsync(log);
    }

    private static async Task AssertOutcomesAsync(FollowerLog log)
    {
        for (byte index = 1; index <= Committed; index++)
        {
            _ = await Assert.That(log.Idempotency.Lookup("client", OperationId(index), [index], out var record)).IsEqualTo(GroupIdempotencyLookup.Found);
            await SequenceAssert.EqualAsync<byte>([index, index], record.OutcomePayload.ToArray());
        }
    }

    private static async Task AssertUnchangedAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var retention = await RetentionAsync(log, cancellationToken);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Ready);
        _ = await Assert.That(log.SnapshotPath).IsNull();
        _ = await Assert.That((retention.RetainedEntries, retention.SnapshotIndex)).IsEqualTo((EntryCount, 0UL));
    }

    private static string OperationId(byte index) => "op-" + index;

    private static ValueTask<FollowerLogRetention> RetentionAsync(IFollowerLog log, CancellationToken cancellationToken) => log.GetRetentionAsync(cancellationToken);

    /// <summary>Opens a log holding entries 1 to 3, committed and applied, each with its resolved outcome.</summary>
    /// <param name="dir">The node data directory.</param>
    /// <param name="options">The log options, or <see langword="null" /> for the defaults.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The open log.</returns>
    private static async Task<FollowerLog> SeedAsync(string dir, FollowerLogOptions? options, CancellationToken cancellationToken)
    {
        var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, options);
        try
        {
            await log.OpenAsync(cancellationToken);
            for (byte index = 1; index <= Committed; index++)
            {
                _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", index, 1UL, "value-" + index), cancellationToken);
                _ = log.Idempotency.Reserve("client", OperationId(index), [index], GroupRecordKind.UserMutation, index, 1UL);
                _ = log.Idempotency.TryResolve("client", OperationId(index), [index, index], index, 1UL);
            }

            _ = await log.AdvanceCommitAsync(Committed, cancellationToken);
            _ = await log.AdvanceAppliedAsync(Committed, cancellationToken);
            return log;
        }
        catch
        {
            await log.DisposeAsync();
            throw;
        }
    }
}
