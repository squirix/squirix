using System;
using System.Globalization;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence;

/// <summary>Unit tests covering automatic retention cleanup of snapshots and journal segments.</summary>
public sealed class RetentionPolicyTests : UnitTestBase, IAsyncLifetime
{
    private TempDirectory? _dir;

    private TempDirectory Dir => _dir ?? throw new InvalidOperationException("Test directory is not initialized.");

    /// <summary>Verifies journal segments older than the current snapshot replay point are removed.</summary>
    [Fact]
    public async Task WriteCleansUpJournalSegmentsOlderThanReplayPoint()
    {
        var options = ManifestStoreTestSupport.CreateOptions(Dir);
        using var store = new ManifestStore(options);

        CreateJournalSegment(1);
        CreateJournalSegment(2);
        CreateJournalSegment(3);
        CreateSnapshot(4);

        await store.WriteAsync(
            new Storage.Manifest.ManifestState
            {
                CurrentJournal = 3,
                LastSnapshot = new Storage.Manifest.ManifestState.SnapshotRef
                {
                    Index = 4,
                    Path = SnapshotPath(4),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 40,
                    ReplayFromJournalSegment = 3,
                },
            },
            DefaultCancellationToken);

        await ManifestStoreTestSupport.WaitUntilAsync(() => !FileKit.Exists(JournalPath(1)) && !FileKit.Exists(JournalPath(2)), TimeSpan.FromSeconds(5), DefaultCancellationToken);

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
        using var store = new ManifestStore(options);

        CreateSnapshot(1);
        CreateSnapshot(2);
        CreateSnapshot(3);

        await store.WriteAsync(
            new Storage.Manifest.ManifestState
            {
                LastSnapshot = new Storage.Manifest.ManifestState.SnapshotRef
                {
                    Index = 3,
                    Path = SnapshotPath(3),
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 30,
                    ReplayFromJournalSegment = 3,
                },
            },
            DefaultCancellationToken);

        await ManifestStoreTestSupport.WaitUntilAsync(() => !FileKit.Exists(SnapshotPath(1)), TimeSpan.FromSeconds(5), DefaultCancellationToken);

        Assert.False(FileKit.Exists(SnapshotPath(1)));
        Assert.True(FileKit.Exists(SnapshotPath(2)));
        Assert.True(FileKit.Exists(SnapshotPath(3)));
    }

    /// <summary>Cleans up the temporary storage directory after each test.</summary>
    public ValueTask DisposeAsync()
    {
        _dir?.Dispose();
        return ValueTask.CompletedTask;
    }

    /// <summary>Creates a fresh temporary storage directory before each test.</summary>
    public ValueTask InitializeAsync()
    {
        _dir = new TempDirectory("squirix");
        return ValueTask.CompletedTask;
    }

    private void CreateJournalSegment(int index) => FileKit.WriteAllText(JournalPath(index), $"journal-{index.ToString(CultureInfo.InvariantCulture)}");

    private void CreateSnapshot(int index) => FileKit.WriteAllText(SnapshotPath(index), $"snapshot-{index.ToString(CultureInfo.InvariantCulture)}");

    private string JournalPath(int index) => PathKit.Combine(
        false,
        Dir,
        $"{StorageFilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");

    private string SnapshotPath(int index) => PathKit.Combine(
        false,
        Dir,
        $"{StorageFilePrefixes.Snapshot}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Snapshot}");
}
