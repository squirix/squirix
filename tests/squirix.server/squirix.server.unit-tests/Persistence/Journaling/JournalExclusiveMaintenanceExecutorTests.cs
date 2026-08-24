using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Ensures the pipelined journal coordinator exposes the same exclusive-maintenance entry point through <see cref="IExclusiveMaintenanceExecutor" /> used by hosted compaction.
/// </summary>
[Immutable]
public sealed class JournalExclusiveMaintenanceExecutorTests : IsolatedStorageTestBase
{
    /// <summary>Verifies dispatch through the interface runs the supplied callback (same gate semantics as a direct coordinator call).</summary>
    [Fact]
    public async Task MaintenanceExecutorRunsGivenAction()
    {
        var persistence = new PersistenceOptions
        {
            DataDir = Dir,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 100,
        };

        using var manifestStore = new Ledger(persistence);
        await using var journal = await JournalCoordinatorFactory.CreateAsync(
            persistence,
            await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken),
            manifestStore,
            new JournalStartupGate(),
            DefaultCancellationToken);
        var executed = new ExecutionFlag();
        await journal.ExecuteMaintenanceExclusiveAsync(executed.MarkExecutedAsync, DefaultCancellationToken);

        Assert.True(executed.WasExecuted);
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
