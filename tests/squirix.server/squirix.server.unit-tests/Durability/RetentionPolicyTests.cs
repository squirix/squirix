using System;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Xunit;

namespace Squirix.Server.UnitTests.Durability;

/// <summary>
/// Unit tests covering automatic retention cleanup of snapshots and journal segments.
/// </summary>
public sealed class RetentionPolicyTests : ServerUnitTestBase, IAsyncLifetime
{
    private string _dir = null!;

    /// <summary>
    /// Verifies only the newest configured snapshot files are kept after manifest persistence.
    /// </summary>
    [Fact]
    public void WriteCleansUpSnapshotsBeyondRetentionCount()
    {
        var options = new PersistenceOptions
        {
            DataDir = _dir,
            SnapshotRetentionCount = 2,
        };
        var store = new ManifestStore(options);

        CreateSnapshot(1);
        CreateSnapshot(2);
        CreateSnapshot(3);

        store.Write(
            new Manifest
            {
                LastSnapshot = new Manifest.SnapshotRef
                {
                    Index = 3,
                    Path = SnapshotPath(3),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 30,
                    ReplayFromJournalSegment = 3,
                },
            });

        Assert.False(FileKit.Exists(SnapshotPath(1)));
        Assert.True(FileKit.Exists(SnapshotPath(2)));
        Assert.True(FileKit.Exists(SnapshotPath(3)));
    }

    /// <summary>
    /// Verifies journal segments older than the current snapshot replay point are removed.
    /// </summary>
    [Fact]
    public void WriteCleansUpJournalSegmentsOlderThanReplayPoint()
    {
        var options = new PersistenceOptions
        {
            DataDir = _dir,
        };
        var store = new ManifestStore(options);

        CreateJournalSegment(1);
        CreateJournalSegment(2);
        CreateJournalSegment(3);
        CreateSnapshot(4);

        store.Write(
            new Manifest
            {
                CurrentJournal = 3,
                LastSnapshot = new Manifest.SnapshotRef
                {
                    Index = 4,
                    Path = SnapshotPath(4),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 40,
                    ReplayFromJournalSegment = 3,
                },
            });

        Assert.False(FileKit.Exists(JournalPath(1)));
        Assert.False(FileKit.Exists(JournalPath(2)));
        Assert.True(FileKit.Exists(JournalPath(3)));
    }

    /// <summary>
    /// Cleans up the temporary storage directory after each test.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
    public ValueTask DisposeAsync()
    {
        DirectoryKit.TryDeleteDirectory(_dir);
        return ValueTask.CompletedTask;
    }

    /// <summary>
    /// Creates a fresh temporary storage directory before each test.
    /// </summary>
    /// <returns>A <see cref="ValueTask" /> representing the asynchronous operation.</returns>
    public ValueTask InitializeAsync()
    {
        _dir = DirectoryKit.CreateTempDirectory("squirix");
        return ValueTask.CompletedTask;
    }

    private void CreateSnapshot(int index) => FileKit.WriteAllText(SnapshotPath(index), $"snapshot-{index}");

    private void CreateJournalSegment(int index) => FileKit.WriteAllText(JournalPath(index), $"journal-{index}");

    private string SnapshotPath(int index) => PathKit.Combine(false, _dir, $"{StorageFilePrefixes.Snapshot}{index:000000}{StorageFileExtensions.Snapshot}");

    private string JournalPath(int index) => PathKit.Combine(false, _dir, $"{StorageFilePrefixes.Journal}{index:000000}{StorageFileExtensions.Journal}");
}
