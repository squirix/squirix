using System;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests covering automatic retention cleanup of snapshots and journal segments.</summary>
public sealed class RetentionPolicyTests : IsolatedStorageTestBase
{
    /// <summary>Verifies journal segments older than the current snapshot replay point are removed.</summary>
    [Fact]
    public async Task WriteCleansUpJournalSegmentsOlderThanReplayPoint()
    {
        var options = StoreTestSupport.CreateOptions(Dir);
        using var store = new Ledger(options);

        CreateJournalSegment(1);
        CreateJournalSegment(2);
        CreateJournalSegment(3);
        CreateSnapshot(4);

        await store.WriteAsync(
            new State
            {
                CurrentJournal = 3,
                LastSnapshot = new SnapshotRef
                {
                    Index = 4,
                    Path = SnapshotPath(4),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 40,
                    ReplayFromJournalSegment = 3,
                },
            },
            DefaultCancellationToken);

        var staleJournalPaths = (JournalPath(1), JournalPath(2));

        // The background retention worker drains asynchronously; give the cleanup the same explicit window the burst
        // tests use under parallel CI load.
        await staleJournalPaths.WaitUntilAsync(
            static paths => !FileKit.Exists(paths.Item1) && !FileKit.Exists(paths.Item2),
            TimeSpan.FromSeconds(30),
            DefaultCancellationToken);

        Assert.False(FileKit.Exists(JournalPath(1)));
        Assert.False(FileKit.Exists(JournalPath(2)));
        Assert.True(FileKit.Exists(JournalPath(3)));
    }

    /// <summary>Verifies only the newest configured snapshot files are kept after manifest persistence.</summary>
    [Fact]
    public async Task WriteCleansUpSnapshotsBeyondRetentionCount()
    {
        var options = new PersistenceOptions
        {
            DataDir = Dir,
            SnapshotRetentionCount = 2,
        };
        using var store = new Ledger(options);

        CreateSnapshot(1);
        CreateSnapshot(2);
        CreateSnapshot(3);

        await store.WriteAsync(
            new State
            {
                LastSnapshot = new SnapshotRef
                {
                    Index = 3,
                    Path = SnapshotPath(3),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 30,
                    ReplayFromJournalSegment = 3,
                },
            },
            DefaultCancellationToken);

        var staleSnapshotPath = SnapshotPath(1);

        // The background retention worker drains asynchronously; give the cleanup the same explicit window the burst
        // tests use under parallel CI load.
        await staleSnapshotPath.WaitUntilAsync(static path => !FileKit.Exists(path), TimeSpan.FromSeconds(30), DefaultCancellationToken);

        Assert.False(FileKit.Exists(SnapshotPath(1)));
        Assert.True(FileKit.Exists(SnapshotPath(2)));
        Assert.True(FileKit.Exists(SnapshotPath(3)));
    }

    private void CreateJournalSegment(int index) => FileKit.WriteAllText(JournalPath(index), $"journal-{NodeInvariantIndexStrings.Format(index)}");

    private void CreateSnapshot(int index) => FileKit.WriteAllText(SnapshotPath(index), $"snapshot-{NodeInvariantIndexStrings.Format(index)}");

    private string JournalPath(int index) => NodePathKit.Combine(false, Dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");

    private string SnapshotPath(int index) => NodePathKit.Combine(false, Dir, $"{FilePrefixes.Snapshot}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Snapshot}");
}
