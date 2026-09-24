using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Persistence.Replication;

/// <summary>Ordering rules of the replication append protocol over the durable follower log.</summary>
[Immutable]
public sealed class FollowerProtocolOrderingTests : NodeIntegrationTestBase
{
    private const string GroupId = "grp-1";

    /// <summary>A divergent uncommitted tail is truncated and rewritten by the new leader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConflictingTailIsTruncatedAndRewritten(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-ordering-conflict");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);

        // Old leader (term 1) appends an entry at index 1, then crashes before a majority.
        var first = await log.AppendAsync(Append(1UL, 1UL, "x"), cancellationToken);
        _ = await Assert.That(first.Success).IsTrue();

        // New leader (term 2) rewrites index 1 with a conflicting entry.
        var result = await log.AppendAsync(Append(1UL, 2UL, "y"), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        var tail = await log.GetUncommittedTailAsync(cancellationToken);
        _ = await Assert.That(tail).HasSingleItem();
        _ = await Assert.That(tail[0].Term).IsEqualTo(2UL);
        _ = await Assert.That(Encoding.UTF8.GetString(tail[0].Payload.Span)).IsEqualTo("y");
    }

    /// <summary>A duplicate batch produces exactly one journal effect.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DuplicateBatchProducesOneJournalEffect(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-ordering-duplicate");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);

        var batch = Batch([Entry(1UL, 1UL, "a"), Entry(2UL, 1UL, "b")], 0UL, 0UL, 1UL);
        var first = await log.AppendAsync(batch, cancellationToken);
        var logLength = FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId));
        var second = await log.AppendAsync(batch, cancellationToken);

        _ = await Assert.That(first.Success).IsTrue();
        _ = await Assert.That(second.Success).IsTrue();
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(2UL);
        _ = await Assert.That(FollowerLogTestKit.GetLogLength(GroupStoragePaths.GetLogPath(dir, GroupId))).IsEqualTo(logLength);
    }

    /// <summary>A higher term is persisted durably before the appending is acknowledged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HigherTermPersistsBeforeResponse(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-ordering-higher-term");

        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId)))
        {
            await log.OpenAsync(cancellationToken);
            _ = await log.AppendAsync(Append(1UL, 1UL, "a"), cancellationToken);

            var higher = new FollowerLogAppendRequest("leader-1", 9UL, 1UL, 1UL, 0UL, ReadOnlyMemory<FollowerLogEntry>.Of(Entry(2UL, 9UL, "b")));
            var result = await log.AppendAsync(higher, cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(9UL);
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await reopened.OpenAsync(cancellationToken);
        _ = await Assert.That((await reopened.GetStatusAsync(cancellationToken)).CurrentTerm).IsEqualTo(9UL);
    }

    /// <summary>An out-of-order batch is rejected without any partial appending.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OutOfOrderBatchRejectedAtomically(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-ordering-gap");

        await using var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId));
        await log.OpenAsync(cancellationToken);

        var result = await log.AppendAsync(Batch([Entry(1UL, 1UL, "a"), Entry(3UL, 1UL, "c")], 0UL, 0UL, 1UL), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(result.RefusalCode).IsEqualTo(FollowerLogRefusal.LogMismatch);
        _ = await Assert.That((await log.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(0UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => Batch([Entry(index, term, payload)], index - 1, index == 1UL ? 0UL : term, term);

    private static FollowerLogAppendRequest Batch(ReadOnlySpan<FollowerLogEntry> entries, ulong prevIndex, ulong prevTerm, ulong term) => new(
        "leader-1",
        term,
        prevIndex,
        prevTerm,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(entries));

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));
}
