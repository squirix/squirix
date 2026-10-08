using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>A higher term seen in a reply is adopted durably, with the vote cleared, before it is reported; nothing else ever changes.</summary>
[Immutable]
public sealed class ObserveTermTests : ServerUnitTestBase
{
    private const string GroupId = "observe-term";

    /// <summary>A higher term is in the metadata file before the call returns, clears the vote, and survives a restart.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermPersistsAndClearsVote(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-observe-term-higher");
        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);

            var observed = await log.ObserveTermAsync(3UL, cancellationToken);

            _ = await Assert.That(observed).IsEqualTo(3UL);
            var meta = await ReadMetaAsync(dir, cancellationToken);
            _ = await Assert.That((meta.CurrentTerm, meta.VotedFor)).IsEqualTo((3UL, string.Empty));
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((3UL, string.Empty));
    }

    /// <summary>A term at or below the durable one changes neither the term nor the vote, and no term step touches the log entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EqualOrLowerTermChangesNothing(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-observe-term-lower");
        await using var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await log.AppendAsync(Append(1UL, 2UL), cancellationToken);
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 1UL, 2UL), cancellationToken);

        var equal = await log.ObserveTermAsync(2UL, cancellationToken);
        var lower = await log.ObserveTermAsync(1UL, cancellationToken);
        var status = await log.GetStatusAsync(cancellationToken);

        _ = await Assert.That((equal, lower)).IsEqualTo((2UL, 2UL));
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, status.LastLogIndex)).IsEqualTo((2UL, "node-b", 1UL));
        _ = await Assert.That(await log.ObserveTermAsync(5UL, cancellationToken)).IsEqualTo(5UL);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
    }

    /// <summary>
    /// A crash after the metadata bytes are written but before they are published fails the log and leaves the old term durable: the
    /// failed log adopts no later term, and a restart recovers the old term with its vote.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CrashBeforePublishKeepsOldTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-observe-term-crash");
        var armed = false;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaWritten().Callback(() =>
        {
            if (armed)
                throw new IOException("simulated crash before the metadata is published.");
        });

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults.Instance()))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
            armed = true;

            _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(log.ObserveTermAsync(3UL, cancellationToken));

            _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
            _ = await Assert.That(await log.ObserveTermAsync(4UL, cancellationToken)).IsEqualTo(2UL);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor)).IsEqualTo((2UL, "node-b"));
    }

    /// <summary>A disposed log adopts no term and reports the durable term it closed with.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposedLogKeepsTerm(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-observe-term-disposed");
        var log = OpenLog(dir);
        await log.OpenAsync(cancellationToken);
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
        await log.DisposeAsync();

        var observed = await log.ObserveTermAsync(5UL, cancellationToken);

        _ = await Assert.That(observed).IsEqualTo(2UL);
        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(2UL);
    }

    /// <summary>
    /// A cancellation that surfaces during the metadata write is no storage failure: the log stays ready, the term is not adopted in
    /// memory or on disk, and a later observation adopts it.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelDuringWriteKeepsLogReady(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-observe-term-cancel");
        var armed = false;
        var faults = new IFollowerLogFaultHooksCreateExpectations();
        _ = faults.Setups.OnFrameWritten();
        _ = faults.Setups.OnFlushed();
        _ = faults.Setups.OnCommitAdvanced();
        _ = faults.Setups.OnBeforeMemoryApply();
        _ = faults.Setups.OnMetaWritten().Callback(() =>
        {
            if (!armed)
                return;

            armed = false;
            throw new OperationCanceledException("simulated cancellation during the metadata write.");
        });

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, faults.Instance());
        await log.OpenAsync(cancellationToken);
        _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 2UL, 0UL, 0UL), cancellationToken);
        armed = true;

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(log.ObserveTermAsync(3UL, cancellationToken));

        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((log.Readiness, status.CurrentTerm, status.VotedFor)).IsEqualTo((FollowerLogReadiness.Ready, 2UL, "node-b"));
        var meta = await ReadMetaAsync(dir, cancellationToken);
        _ = await Assert.That(meta.CurrentTerm).IsEqualTo(2UL);
        _ = await Assert.That(await log.ObserveTermAsync(3UL, cancellationToken)).IsEqualTo(3UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term) => new(
        "node-a",
        term,
        index - 1,
        0UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(index, term, Encoding.UTF8.GetBytes("a"))));

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance);

    private static async Task<GroupLogMetadata> ReadMetaAsync(TempDirectory dir, CancellationToken cancellationToken)
    {
        var bytes = await File.ReadAllBytesAsync(GroupStoragePaths.GetMetadataPath(dir, GroupId), cancellationToken);
        return GroupLogCodec.TryDecodeMeta(bytes, out var meta) ? meta : throw new InvalidDataException("The metadata file does not decode.");
    }
}
