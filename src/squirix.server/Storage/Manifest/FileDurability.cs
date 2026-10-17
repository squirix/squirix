using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

// PERF (Linux): a manifest roll is two writes (data file + two pointer slots) plus fsyncs — a good fit for a
// single batched io_uring submission. That io_uring roll writer was archived after the raw ring caused a
// fatal AccessViolationException on Linux; the portable RandomAccess path below is used on every platform for now.

/// <summary>WAL-ordered durable writes for Manifest data files.</summary>
internal static class FileDurability
{
    internal static void WriteManifestDataFileBlocking(string targetPath, ReadOnlySpan<byte> encoded)
    {
        var options = GetDataFileOptions();
        using var handle = File.OpenHandle(targetPath, FileMode.CreateNew, FileAccess.Write, FileShare.Read, options);
        if (encoded.Length > 0)
        {
            RandomAccess.SetLength(handle, encoded.Length);
            RandomAccess.Write(handle, encoded, 0);
        }

        FlushDataFileIfNeeded(handle);

        // The pointer must never name a file whose directory entry could still be lost; nothing else flushes it now that the pointer is written in place.
        FileEx.FlushDirectoryEntry(targetPath);
    }

    private static void FlushDataFileIfNeeded(SafeFileHandle handle)
    {
        if (!OperatingSystem.IsWindows())
            RandomAccess.FlushToDisk(handle);
    }

    private static FileOptions GetDataFileOptions()
    {
        var options = FileOptions.SequentialScan;
        if (OperatingSystem.IsWindows())
            options |= FileOptions.WriteThrough;

        return options;
    }
}
