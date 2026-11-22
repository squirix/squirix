using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Node.App;

/// <summary>Covers the optional callbacks after durable memory apply.</summary>
[Immutable]
public sealed class DurableMutationPhasesTests : IsolatedStorageTestBase
{
    private static readonly CacheKey Key = CacheKey.Default("a");

    /// <summary>A successful durable mutation runs after-apply.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SuccessfulMutationRunsAfterApply(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        _ = await ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(), cancellationToken);

        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,apply,afterApply");
    }

    /// <summary>A failed apply never runs the after-apply phase.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedApplySkipsAfterApply(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var log = new List<string>();

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(ExecuteAsync(journal, log, DurableMutationCondition<int>.Apply(), cancellationToken, true), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(string.Join(',', log)).IsEqualTo("append,apply");
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
                static (s, _) => s.Log.Add("afterApply")),
            cancellationToken).AsTask();
    }
}
