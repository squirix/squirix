using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.PipelinedWal;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>
/// Ensures <see cref="JournalWriter" /> exposes the same exclusive-maintenance entry point through <see cref="IExclusiveMaintenanceExecutor" /> used by hosted compaction.
/// </summary>
public sealed class JournalWriterExclusiveMaintenanceExecutorTests : UnitTestBase
{
    /// <summary>
    /// Verifies dispatch through the interface runs the supplied callback (same gate semantics as a direct <see cref="JournalWriter" /> call).
    /// </summary>
    [Fact]
    public async Task ExclusiveMaintenanceExecutorDispatchRunsSuppliedAction()
    {
        using var dir = new TempDirectory("squirix-journal-maint-iface");
        var persistence = new PersistenceOptions
        {
            DataDir = dir,
            JournalBackend = JournalBackend.JsonFramed,
            JournalMaxSegmentMb = 1,
            FlushIntervalMs = 100,
        };

        using var manifestStore = new ManifestStore(persistence);
        await using var journal = await JournalWriter.CreateAsync(persistence, await manifestStore.ReadCurrentOrDefaultAsync(DefaultCancellationToken), manifestStore, new JournalStartupGate(), DefaultCancellationToken);
        var executed = false;
        await journal.ExecuteMaintenanceExclusiveAsync(
            _ =>
            {
                executed = true;
                return ValueTask.CompletedTask;
            },
            DefaultCancellationToken);

        Assert.True(executed);
    }
}
