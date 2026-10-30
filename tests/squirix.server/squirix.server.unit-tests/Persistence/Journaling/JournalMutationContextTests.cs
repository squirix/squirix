using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Journal acceptance and stamping depend only on the storage-owned operation context.</summary>
[Immutable]
public sealed class JournalMutationContextTests : IsolatedStorageTestBase
{
    private const string ExpectedOperationId = "0123456789abcdef0123456789abcdef";
    private const string ExpectedFingerprint = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";

    /// <summary>Accepted frames carry the context metadata and notify only the matching stamped or unstamped path.</summary>
    /// <param name="stamped">Whether the current operation supplies a stamp.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(true)]
    [Arguments(false)]
    public async Task AcceptedFrameUsesJournalContext(bool stamped, CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, cancellationToken);
        var context = new MutationContext(stamped);
        var previous = JournalMutationContext.Current;
        JournalMutationContext.Current = context;
        try
        {
            await journal.Journal.AppendPutUnderGateAsync(CacheKey.Default("key"), JournalEntryPayloadKit.EncodePut("value"), cancellationToken);
            await journal.Journal.AwaitDurabilityCommitAsync(cancellationToken);
        }
        finally
        {
            JournalMutationContext.Current = previous;
        }

        await journal.ShutdownAsync();
        var mutations = 0;
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
        {
            if (records.Current.Operation != JournalOperationKind.Put)
                continue;

            mutations++;
            _ = await Assert.That(records.Current.MutationOperationId).IsEqualTo(stamped ? ExpectedOperationId : null);
            _ = await Assert.That(records.Current.MutationFingerprint).IsEqualTo(stamped ? ExpectedFingerprint : null);
        }

        _ = await Assert.That(mutations).IsEqualTo(1);
        _ = await Assert.That(context.Stamped).IsEqualTo(stamped ? 1 : 0);
        _ = await Assert.That(context.Applied).IsEqualTo(stamped ? 0 : 1);
    }

    /// <summary>A capacity refusal happens before any accepted-mutation notification.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedFrameDoesNotNotifyJournalContext(CancellationToken cancellationToken)
    {
        await using var journal = await StallableJournal.CreateAsync(Dir, false, 1, cancellationToken);
        var context = new MutationContext(true);
        var previous = JournalMutationContext.Current;
        JournalMutationContext.Current = context;
        try
        {
            _ = await NodeAsyncAssert.ThrowsAsync<JournalCapacityExceededException>(journal.Journal.AppendPutUnderGateAsync(
                CacheKey.Default("oversized"), new byte[(1024 * 1024) + 1024], cancellationToken));
        }
        finally
        {
            JournalMutationContext.Current = previous;
        }

        _ = await Assert.That(context.Stamped).IsEqualTo(0);
        _ = await Assert.That(context.Applied).IsEqualTo(0);
    }

    private sealed class MutationContext : IJournalMutationContext
    {
        internal MutationContext(bool stamped)
        {
            OperationId = stamped ? ExpectedOperationId : null;
            Fingerprint = stamped ? ExpectedFingerprint : null;
        }

        public string? OperationId { get; }

        public string? Fingerprint { get; }

        public bool HasAppendedOutcome => false;

        internal int Stamped { get; private set; }

        internal int Applied { get; private set; }

        public void NotifyMutationStamped() => Stamped++;

        public void NotifyMutationApplied() => Applied++;
    }
}
