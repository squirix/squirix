using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Compaction;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.Storage.Snapshot.Binary;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.UnitTests.Persistence.Manifest;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Documents the snapshot + retention + compaction gap that loses keys only present in pruned segments.</summary>
public sealed class JournalCompactionGapTests : ServerUnitTestBase
{
    private const int PrunedOnlyKeyIndex = 1;

    /// <summary>Key only in a pruned segment is lost when snapshot omits it and replay starts above that segment.</summary>
    [Fact]
    public async Task KeyOnlyInPrunedSegmentIsLostAfterCompactionWhenMissingFromSnapshot()
    {
        using var dir = new TempDirectory("squirix-compact-gap-negative");
        var persistence = JournalCompactionTestSupport.NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);

        var put0 = await JournalCompactionTestSupport.BuildVolumePutAsync(1UL, 0);
        var put1 = await JournalCompactionTestSupport.BuildVolumePutAsync(2UL, PrunedOnlyKeyIndex);
        var put2 = await JournalCompactionTestSupport.BuildVolumePutAsync(3UL, 2);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 1, [put0, put1]);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 2, [put2]);

        var snapshotPath = await JournalCompactionTestSupport.WriteSnapshotAsync(dir, 1, [0]);
        await manifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = new SnapshotRef
                {
                    Index = 1,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 2,
                    ReplayFromJournalSegment = 2,
                },
            },
            DefaultCancellationToken);

        var prunedSegment = NodePathKit.Combine(dir, StoreTestSupport.JournalSegment000001);
        await StoreTestSupport.WaitUntilAsync(
            prunedSegment,
            static path => !FileKit.Exists(path),
            TimeSpan.FromSeconds(5),
            DefaultCancellationToken);
        Assert.False(FileKit.Exists(prunedSegment));

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var lostKey = JournalCompactionTestSupport.FormatKey(PrunedOnlyKeyIndex);
        var (found, _) = JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, lostKey);
        Assert.False(found);
        Assert.True(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(0)).Found);
        Assert.True(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(2)).Found);
    }

    /// <summary>When the pruned segment keys are captured in snapshot, compaction preserves them.</summary>
    [Fact]
    public async Task KeyInPrunedSegmentSurvivesWhenPresentInSnapshot()
    {
        using var dir = new TempDirectory("squirix-compact-gap-positive");
        var persistence = JournalCompactionTestSupport.NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);

        var put0 = await JournalCompactionTestSupport.BuildVolumePutAsync(1UL, 0);
        var put1 = await JournalCompactionTestSupport.BuildVolumePutAsync(2UL, PrunedOnlyKeyIndex);
        var put2 = await JournalCompactionTestSupport.BuildVolumePutAsync(3UL, 2);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 1, [put0, put1]);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 2, [put2]);

        var snapshotPath = await JournalCompactionTestSupport.WriteSnapshotAsync(dir, 1, [0, PrunedOnlyKeyIndex]);
        await manifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = new SnapshotRef
                {
                    Index = 1,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 2,
                    ReplayFromJournalSegment = 2,
                },
            },
            DefaultCancellationToken);

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        var prunedKey = JournalCompactionTestSupport.FormatKey(PrunedOnlyKeyIndex);
        Assert.True(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, prunedKey).Found);
    }

    /// <summary>Missing snapshot file forces journal-only compaction input and drops pruned-segment keys.</summary>
    [Fact]
    public async Task CompactionSkipsMissingSnapshotFileAndLosesPrunedKeys()
    {
        using var dir = new TempDirectory("squirix-compact-missing-snapshot");
        var persistence = JournalCompactionTestSupport.NewPersistence(dir);
        using var manifestStore = new ManifestStore(persistence);

        var put0 = await JournalCompactionTestSupport.BuildVolumePutAsync(1UL, 0);
        var put1 = await JournalCompactionTestSupport.BuildVolumePutAsync(2UL, PrunedOnlyKeyIndex);
        var put2 = await JournalCompactionTestSupport.BuildVolumePutAsync(3UL, 2);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 1, [put0, put1]);
        await JournalCompactionTestSupport.WriteJournalSegmentAsync(dir, 2, [put2]);

        var snapshotPath = await JournalCompactionTestSupport.WriteSnapshotAsync(dir, 1, [0, PrunedOnlyKeyIndex]);
        await manifestStore.WriteAsync(
            new State
            {
                Format = 1,
                CurrentJournal = 2,
                NextSequence = 4,
                LastSnapshot = new SnapshotRef
                {
                    Index = 1,
                    Path = snapshotPath,
                    CreatedUtc = DateTime.UtcNow,
                    LastAppliedSequence = 2,
                    ReplayFromJournalSegment = 2,
                },
            },
            DefaultCancellationToken);

        File.Delete(snapshotPath);

        await JournalCompactor.CompactAsync(persistence, manifestStore, StoreFactory.CreateReader(persistence), DefaultCancellationToken);

        Assert.False(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(0)).Found);
        Assert.False(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(PrunedOnlyKeyIndex)).Found);
        Assert.True(JournalCompactionProbe.FindKeyInJournal(dir, JournalCompactionTestSupport.VolumeNamespace, JournalCompactionTestSupport.FormatKey(2)).Found);
    }
}
