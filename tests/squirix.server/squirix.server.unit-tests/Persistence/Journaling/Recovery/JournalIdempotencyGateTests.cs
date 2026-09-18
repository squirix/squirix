using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Idempotency-outcome appends must serialize behind the mutation gate (issue #419).</summary>
[Immutable]
public sealed class JournalIdempotencyGateTests : IsolatedStorageTestBase
{
    private const string Fingerprint = "try-add-entry-async|default|gate-key|abc123";
    private const string OperationId = "0123456789abcdef0123456789abcdef";

    /// <summary>
    /// While the mutation gate is held, an idempotency-outcome appending must not be enqueued: bypassing the gate, let
    /// just-acked frames be deleted by compaction. With the fix the appending blocks on
    /// <see cref="IJournalCoordinatorSnapshotState.MutationGate" /> and only advances the journal sequence after the
    /// gate is released, so it never races a segment roll or publish.
    /// </summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task IdempotencyAppendWaitsForMutationGate(CancellationToken cancellationToken)
    {
        var persistence = CreatePersistence(Dir);
        using var ledger = new Ledger(persistence);
        var manifest = await ledger.ReadCurrentOrDefaultAsync(cancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(persistence, manifest, ledger, new AsyncManualResetEvent(true));

        var snapshotState = (await Assert.That(journal).IsTypeOf<IJournalCoordinatorSnapshotState>())!;
        var gateGuard = await snapshotState.MutationGate.LockAsync(cancellationToken);

        var responseBytes = IdempotencyResponseCodec.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        var initialSequence = journal.NextSequence;
        var appendTask = journal.AppendIdempotencyOutcomeAsync(OperationId, Fingerprint, responseBytes, cancellationToken).AsTask();

        // The appending is gated: it has not been enqueued, so the journal sequence has not advanced.
        _ = await Assert.That(appendTask.IsCompleted).IsFalse();
        _ = await Assert.That(journal.NextSequence).IsEqualTo(initialSequence);

        gateGuard.Dispose();
        await appendTask;
        _ = await Assert.That(journal.NextSequence).IsNotEqualTo(initialSequence);
    }

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };
}
