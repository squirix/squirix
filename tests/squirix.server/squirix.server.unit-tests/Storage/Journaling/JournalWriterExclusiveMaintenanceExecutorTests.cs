using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage.Journaling;

/// <summary>
/// Ensures <see cref="JournalWriter" /> exposes the same exclusive-maintenance entry point through <see cref="IExclusiveMaintenanceExecutor" /> used by hosted compaction.
/// </summary>
public sealed class JournalWriterExclusiveMaintenanceExecutorTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies dispatch through the interface runs the supplied callback (same gate semantics as a direct <see cref="JournalWriter" /> call).
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous unit test.</returns>
    [Fact]
    public async Task ExclusiveMaintenanceExecutorDispatchRunsSuppliedAction()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-journal-maint-iface");
        try
        {
            var persistence = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 100,
            };

            var manifestStore = new ManifestStore(persistence);
            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
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
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
