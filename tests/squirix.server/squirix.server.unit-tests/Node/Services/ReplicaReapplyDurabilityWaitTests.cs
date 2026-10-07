using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Node.App.Decorators;
using Squirix.Server.Node.Services;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaOwnerTestKit;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>
/// The leader re-applies committed replica entries to a journaled local cache without waiting for the flush of each entry's own node journal
/// frame, and the durable applied index still advances only behind the node journal durability barrier.
/// </summary>
[Immutable]
public sealed class ReplicaReapplyDurabilityWaitTests : IsolatedStorageTestBase
{
    private static readonly TimeSpan StallTimeout = TimeSpan.FromSeconds(10);

    /// <summary>The durable applied index stays behind a stalled node journal flush and reaches the applied entries once the flush completes.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task FlushAppliedWaitsForStalledFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new StubCache();
        var local = CreateJournaledCache(journal, memory);
        await using var registry = await OpenRegistryAsync(GroupDir(), cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), local);
        journal.Writer.Flush.Arm();
        FlushState stalled;
        FlushState flushed;
        string frames;
        try
        {
            await WriteAsync(committer, ["k1", "k2", "k3"], cancellationToken);
            var flush = committer.FlushAppliedAsync(journal.Journal, cancellationToken);
            await journal.Writer.Flush.Entered.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            stalled = new FlushState(flush.IsCompleted, (await StatusAsync(registry, cancellationToken)).LastAppliedIndex);
            journal.Writer.Flush.Release();
            await flush.WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            flushed = new FlushState(flush.IsCompleted, (await StatusAsync(registry, cancellationToken)).LastAppliedIndex);
            frames = journal.ReadStampedPuts(cancellationToken);
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(stalled).IsEqualTo(new FlushState(false, 0UL));
        _ = await Assert.That(flushed).IsEqualTo(new FlushState(true, 3UL));
        _ = await Assert.That(frames).IsEqualTo(StallableJournal.Describe([new CacheKey("cache", "k1").ToString(), new CacheKey("cache", "k2").ToString(), new CacheKey("cache", "k3").ToString(), CacheKey.Default("w").ToString()]));
        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], memory.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>A restarted owner catches memory up with the committed entries above the durable applied index while the node journal flush is stalled.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RestartCatchUpDoesNotWaitForFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using (var registry = await OpenRegistryAsync(GroupDir(), cancellationToken))
        {
            await using var committer = CreateCommitter(registry, new ScriptedGateway());
            await WriteAsync(committer, ["k1", "k2", "k3"], cancellationToken);
        }

        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new StubCache();
        await using var restarted = await OpenRegistryAsync(GroupDir(), cancellationToken);
        await using var owner = CreateCommitter(restarted, new ScriptedGateway(), CreateJournaledCache(journal, memory));
        journal.Writer.Flush.Arm();
        bool flushEntered;
        try
        {
            await owner.CommitSetAsync(NewOperationId(), "cache", "k4", Entry("k4"), cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            flushEntered = journal.Writer.Flush.Entered.IsCompleted;
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(flushEntered).IsFalse();
        await SequenceAssert.EqualAsync(["k1", "k2", "k3", "k4"], memory.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>The retained entry a later write applies on behalf of the earlier one does not wait for its node journal flush either.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ForeignApplyDoesNotWaitForFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new StubCache();
        var journaled = CreateJournaledCache(journal, memory);
        var failing = 1;
        var flaky = new ILogicalNamespacedCacheCreateExpectations<object?>();
        _ = flaky.Setups.SetEntryAsync(Arg.Any<string>(), Arg.Any<string>(), Arg.Any<string>(), Arg.Any<NodeCacheEntry<object?>>(), Arg.Any<CancellationToken>())
                 .Callback(
                     (operationId, cacheName, key, entry, token) => Interlocked.Exchange(ref failing, 0) == 1
                         ? ValueTask.FromException(new IOException("Injected memory apply failure after the majority."))
                         : journaled.SetEntryAsync(operationId, cacheName, key, entry, token));
        await using var registry = await OpenRegistryAsync(GroupDir(), cancellationToken);
        await using var committer = CreateCommitter(registry, new ScriptedGateway(), flaky.Instance());
        var first = await NodeAsyncAssert.ThrowsAsync<SquirixException>(committer.CommitSetAsync(NewOperationId(), "cache", "k1", Entry("k1"), cancellationToken));
        journal.Writer.Flush.Arm();
        bool flushEntered;
        try
        {
            await committer.CommitSetAsync(NewOperationId(), "cache", "k2", Entry("k2"), cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            flushEntered = journal.Writer.Flush.Entered.IsCompleted;
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(first.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(flushEntered).IsFalse();
        await SequenceAssert.EqualAsync(["k1", "k2"], memory.Applied.ToArray(), StringComparer.Ordinal);
    }

    /// <summary>Re-applying committed entries on a journaled cache completes while the node journal flush is stalled, and memory holds every entry.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReapplyDoesNotWaitForFlush(bool groupCommit, CancellationToken cancellationToken)
    {
        await using var journal = await CreateWarmJournalAsync(groupCommit, cancellationToken);
        var memory = new StubCache();
        var applier = new ReplicaGroupApplier(CreateJournaledCache(journal, memory), NullLogger.Instance);
        var factory = new ReplicaMutationFactory(new StubCache(), "n1", 1UL, TimeProvider.System, NullLogger.Instance);
        byte[][] payloads =
        [
            factory.PrepareSet(NewOperationId(), "cache", "k1", Entry("k1"), 1UL).CanonicalPayload.ToArray(),
            factory.PrepareSet(NewOperationId(), "cache", "k2", Entry("k2"), 2UL).CanonicalPayload.ToArray(),
            factory.PrepareSet(NewOperationId(), "cache", "k3", Entry("k3"), 3UL).CanonicalPayload.ToArray(),
        ];
        journal.Writer.Flush.Arm();
        bool flushEntered;
        try
        {
            await ReapplyAsync(applier, payloads, cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
            flushEntered = journal.Writer.Flush.Entered.IsCompleted;
        }
        finally
        {
            journal.Writer.ReleaseAll();
        }

        _ = await Assert.That(flushEntered).IsFalse();
        _ = await Assert.That(applier.AppliedIndex).IsEqualTo(3UL);
        await SequenceAssert.EqualAsync(["k1", "k2", "k3"], memory.Applied.ToArray(), StringComparer.Ordinal);
    }

    private static JournalLoggingCacheDecorator<object?> CreateJournaledCache(StallableJournal journal, ILogicalNamespacedCache<object?> memory) =>
        new(memory, journal.Journal, new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance));

    private static async Task ReapplyAsync(ReplicaGroupApplier applier, byte[][] payloads, CancellationToken cancellationToken)
    {
        var logIndex = 0UL;
        foreach (var payload in payloads)
        {
            logIndex++;
            await applier.ApplyAsync(logIndex, payload, cancellationToken);
        }
    }

    private static async Task WriteAsync(ReplicaGroupCommitter committer, string[] keys, CancellationToken cancellationToken)
    {
        foreach (var key in keys)
            await committer.CommitSetAsync(NewOperationId(), "cache", key, Entry(key), cancellationToken).WaitAsync(StallTimeout, TimeProvider.System, cancellationToken);
    }

    private string GroupDir() => Directory.CreateDirectory(Path.Join(Dir, "group")).FullName;

    /// <summary>Creates a journal whose segment header and a first frame are already durable, so a later stall catches only the frames under test.</summary>
    /// <param name="groupCommit">Whether journal group commit is enabled.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The started journal.</returns>
    private async Task<StallableJournal> CreateWarmJournalAsync(bool groupCommit, CancellationToken cancellationToken)
    {
        var journal = await StallableJournal.CreateAsync(Directory.CreateDirectory(Path.Join(Dir, "node")).FullName, groupCommit, cancellationToken);
        try
        {
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("w"), JournalEntryPayloadKit.EncodePut("w"), cancellationToken);
            await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
            return journal;
        }
        catch
        {
            await journal.DisposeAsync();
            throw;
        }
    }

    private readonly record struct FlushState(bool FlushCompleted, ulong LastAppliedIndex);
}
