using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Runtime;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>The optional after-append and after-apply phases of a durable mutation run at their points and only when they apply.</summary>
[Immutable]
public sealed class DurableMutationPhasesTests : IsolatedStorageTestBase
{
    private static readonly CacheKey Key = CacheKey.Default("a");

    /// <summary>A predicted mutation runs its phases in order: append, after-append with the prediction, apply, after-apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PredictedMutationRunsAllPhasesInOrder(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        var result = await ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(7), cancellationToken);

        _ = await Assert.That(result).IsEqualTo(1);
        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,afterAppend:7,apply,afterApply");
    }

    /// <summary>A mutation whose precondition predicted nothing skips the after-append phase but still runs after-apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnpredictedMutationSkipsAfterAppend(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        _ = await ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(), cancellationToken);

        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,apply,afterApply");
    }

    /// <summary>A write whose durable source is not the cache journal never runs the after-append phase.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuspendedStampingSkipsAfterAppend(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        using (RpcMutationIdempotencyExecutionAmbient.SuspendStamping())
            _ = await ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(7), cancellationToken);

        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,apply,afterApply");
    }

    /// <summary>A failed apply never runs the after-apply phase.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedApplySkipsAfterApply(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(7), cancellationToken, true));

        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,afterAppend:7,apply");
    }

    /// <summary>A failure of the after-append phase is an unknown outcome, the apply never runs, and the journal is latched so nothing it appended can be flushed unapplied.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AfterAppendFailureIsUnknown(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);

        var error = await NodeAsyncAssert.ThrowsAsync<SquirixException>(
            executor.ExecuteAsync(
                Key,
                static (_, _) => ValueTask.FromResult(DurableMutationCondition<int>.Apply(7)),
                new DurableMutationPipeline<(IJournalCoordinator Journal, List<string> Log), int>(
                    (journal.Journal, log),
                    static (s, ownership, ct) => s.Journal.AppendPutAsync(ownership, Key, JournalEntryPayloadKit.EncodePut("a"), ct),
                    static (s, _) =>
                    {
                        s.Log.Add("apply");
                        return ValueTask.FromResult(1);
                    },
                    static (_, _) => ValueTask.FromException(new InvalidOperationException("after-append failed"))),
                cancellationToken).AsTask());

        _ = await Assert.That(error.Code).IsEqualTo(SquirixErrorCode.CommitOutcomeUnknown);
        _ = await Assert.That(log).IsEmpty();
        _ = await Assert.That(journal.Journal.GetJournalThreadFailure()).IsNotNull();
    }

    private static Task<int> ExecuteAsync(StallableJournal journal, List<string> log, DurableMutationCondition<int> condition, CancellationToken cancellationToken, bool fail = false)
    {
        var executor = new DurableMutationExecutor(journal.Journal, NullLogger<DurableMutationExecutor>.Instance);
        return executor.ExecuteAsync(
            Key,
            static (s, _) => ValueTask.FromResult(s.Condition),
            new DurableMutationPipeline<(IJournalCoordinator Journal, List<string> Log, bool Fail, DurableMutationCondition<int> Condition), int>(
                (journal.Journal, log, fail, condition),
                static (s, ownership, ct) =>
                {
                    s.Log.Add("append");
                    return s.Journal.AppendPutAsync(ownership, Key, JournalEntryPayloadKit.EncodePut("a"), ct);
                },
                static (s, _) =>
                {
                    s.Log.Add("apply");
                    return s.Fail ? ValueTask.FromException<int>(new InvalidOperationException("apply failed")) : ValueTask.FromResult(1);
                },
                static (s, predicted) =>
                {
                    s.Log.Add($"afterAppend:{predicted}");
                    return ValueTask.CompletedTask;
                },
                static (s, _) => s.Log.Add("afterApply")),
            cancellationToken).AsTask();
    }
}
