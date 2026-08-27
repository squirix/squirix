using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using Squirix.Transport.Grpc.Cache;
using Xunit;

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
    [Fact]
    public async Task IdempotencyAppendWaitsForMutationGate()
    {
        var persistence = CreatePersistence(Dir.Path);
        using var ledger = new Ledger(persistence);
        var manifest = await ledger.ReadCurrentOrDefaultAsync(DefaultCancellationToken);
        await using var journal = JournalCoordinatorFactory.Create(persistence, manifest, ledger, new AsyncManualResetEvent(true));

        var snapshotState = Assert.IsAssignableFrom<IJournalCoordinatorSnapshotState>(journal);
        var gateGuard = await snapshotState.MutationGate.LockAsync(DefaultCancellationToken);

        var responseBytes = RpcMutationIdempotencyStore.SerializeResponseBytes(new TryAddAsyncResponse { Added = true });
        var initialSequence = journal.NextSequence;
        var appendTask = journal.AppendIdempotencyOutcomeAsync(OperationId, Fingerprint, responseBytes, DefaultCancellationToken).AsTask();

        // The appending is gated: it has not been enqueued, so the journal sequence has not advanced.
        Assert.False(appendTask.IsCompleted);
        Assert.Equal(initialSequence, journal.NextSequence);

        gateGuard.Dispose();
        await appendTask;
        Assert.NotEqual(initialSequence, journal.NextSequence);
    }

    private static PersistenceOptions CreatePersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushIntervalMs = 5 };
}
