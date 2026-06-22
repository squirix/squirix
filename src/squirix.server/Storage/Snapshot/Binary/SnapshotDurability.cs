using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Snapshot.Binary;

/// <summary>Durable temp-file options and flush helpers for binary snapshot writes.</summary>
internal static class SnapshotDurability
{
    internal static FileOptions GetTempFileOptions()
    {
        var options = FileOptions.SequentialScan | FileOptions.Asynchronous;
        if (OperatingSystem.IsWindows())
            options |= FileOptions.WriteThrough;

        return options;
    }

    internal static void FlushIfNeeded(SafeFileHandle handle)
    {
        if (!OperatingSystem.IsWindows())
            RandomAccess.FlushToDisk(handle);
    }
}
