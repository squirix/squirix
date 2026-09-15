using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Ensures the pipelined journal coordinator exposes the same exclusive-maintenance entry point through <see cref="IExclusiveMaintenanceExecutor" /> used by hosted compaction.
/// </summary>
[Immutable]
public sealed class JournalExclusiveMaintenanceExecutorTests : IsolatedStorageTestBase
{
    /// <summary>Verifies dispatch through the interface runs the supplied callback (same gate semantics as a direct coordinator call).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MaintenanceExecutorRunsGivenAction(CancellationToken cancellationToken)
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushInterval = 100,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = JournalCoordinatorFactory.Create(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(cancellationToken),
            manifestStore,
            new AsyncManualResetEvent(true));
        var executed = new ExecutionFlag();
        await journal.ExecuteMaintenanceExclusiveAsync(executed.MarkExecutedAsync, cancellationToken);

        _ = await Assert.That(executed.WasExecuted).IsTrue();
    }

    private sealed class ExecutionFlag
    {
        internal bool WasExecuted { get; private set; }

        internal ValueTask MarkExecutedAsync(CancellationToken cancellationToken)
        {
            _ = cancellationToken;
            WasExecuted = true;
            return ValueTask.CompletedTask;
        }
    }
}
