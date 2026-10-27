using System;
using System.IO;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Helpers for manifest store tests.</summary>
internal static class StoreTestSupport
{
    internal const string JournalSegment000001 = $"{FilePrefixes.Journal}000001{FileExtensions.Journal}";
    internal const string JournalSegment000002 = $"{FilePrefixes.Journal}000002{FileExtensions.Journal}";
    internal const string JournalSegment000003 = $"{FilePrefixes.Journal}000003{FileExtensions.Journal}";
    internal const string Snapshot000001 = $"{FilePrefixes.Snapshot}000001{FileExtensions.Snapshot}";
    internal const string Snapshot000002 = $"{FilePrefixes.Snapshot}000002{FileExtensions.Snapshot}";

    internal static PersistenceOptions CreateOptions(string dataDir) => new()
    {
        DataDir = dataDir,
    };

    internal static SlotSelection ReadCurrentSelection(string dataDir) => SlotFile.Read(CurrentPath(dataDir));

    internal static State ReadSlotManifest(string dataDir, int slot)
    {
        using var handle = File.OpenHandle(CurrentPath(dataDir), FileMode.Open, FileAccess.Read, SlotFile.CompatibleShare);
        var record = GC.AllocateArray<byte>(SlotFormat.RecordSize);
        long offset = slot == 0 ? 0 : SlotFormat.SlotSize;
        _ = RandomAccess.Read(handle, record, offset);
        return SlotFormat.Select(record, [], SlotFormat.FileSize, CurrentPath(dataDir)).State;
    }

    private static string CurrentPath(string dataDir) => Path.Join(dataDir, $"{FilePrefixes.Manifest}current");
}
