using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Storage;

/// <summary>
/// Verifies journal snapshot cut error paths release the mutation gate.
/// </summary>
public sealed class JournalWriterSnapshotCutReleaseTests : ServerUnitTestBase
{
    /// <summary>
    /// Verifies journal mutation path is usable after a snapshot cut action throws.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task SnapshotCutFailureStillAllowsJournalAppend()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-cut-fail");
        try
        {
            var persistence = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 5,
                ManifestRetentionCount = 1,
            };

            var manifestStore = new ManifestStore(persistence);
            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());

            var payload = DiscriminatedEntryJsonWriter.BuildEntryJson("v", null, null, 1, null);
            await journal.AppendPutAsync(CacheKey.Default("before"), payload, null, DefaultCancellationToken);

            _ = await Assert.ThrowsAsync<IOException>(() => journal.ExecuteSnapshotCutAsync(
                0,
                static (_, _, _) => ValueTask.FromException<Manifest.SnapshotRef>(new IOException("simulated snapshot failure")),
                DefaultCancellationToken).AsTask());

            await journal.AppendPutAsync(CacheKey.Default("after"), payload, null, DefaultCancellationToken);
            await journal.AwaitDurabilityCommitAsync(DefaultCancellationToken);

            Assert.Equal(2, journal.AppendedOps);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }

    /// <summary>
    /// Ensures a snapshot cut cannot record a journal sequence while a durable mutation is still pending memory apply.
    /// </summary>
    /// <returns>A <see cref="Task" /> representing the test.</returns>
    [Fact]
    public async Task SnapshotCutWaitsForPendingMemoryApply()
    {
        var dir = DirectoryKit.CreateTempDirectory("squirix-snap-cut-pending-apply");
        try
        {
            var persistence = new PersistenceOptions
            {
                DataDir = dir,
                JournalMaxSegmentMb = 1,
                FlushIntervalMs = 600_000,
                ManifestRetentionCount = 1,
            };

            var manifestStore = new ManifestStore(persistence);
            await using var journal = new JournalWriter(persistence, manifestStore.ReadCurrentOrDefault(), manifestStore, new JournalStartupGate());
            var snapshotStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

            journal.BeginPendingMemoryApply();
            var snapshotTask = journal.ExecuteSnapshotCutAsync(
                snapshotStarted,
                static (started, _, _) =>
                {
                    started.SetResult();
                    return new ValueTask<int>(1);
                },
                DefaultCancellationToken).AsTask();

            var first = await Task.WhenAny(snapshotStarted.Task, Task.Delay(TimeSpan.FromMilliseconds(50), DefaultCancellationToken));
            Assert.NotSame(snapshotStarted.Task, first);

            journal.CompletePendingMemoryApply();

            Assert.Equal(1, await snapshotTask.WaitAsync(TimeSpan.FromSeconds(5), DefaultCancellationToken));
            Assert.True(snapshotStarted.Task.IsCompletedSuccessfully);
        }
        finally
        {
            DirectoryKit.TryDeleteDirectory(dir);
        }
    }
}
