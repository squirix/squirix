using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Manifest;

// PERF (Linux): a manifest roll is two writes (data file + fixed pointer) plus fsyncs — a good fit for a
// single batched io_uring submission. That io_uring roll writer was archived after the raw ring caused a
// fatal AccessViolationException on Linux; the portable RandomAccess path below is used on every platform for now.

/// <summary>WAL-ordered durable writes for Manifest data files and the fixed-size CURRENT pointer.</summary>
internal static class FileDurability
{
    internal static void FlushPointerIfNeeded(SafeFileHandle handle) => RandomAccess.FlushToDisk(handle);

    internal static FileOptions GetPointerFileOptions() => FileOptions.None;

    /// <summary>Overwrites the fixed-size SQMC pointer in place.</summary>
    /// <param name="writer">Pointer writer with an open or reusable <c language="csharp">man-current</c> handle.</param>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC bytes.</param>
    /// <exception cref="ArgumentException">Thrown when <paramref name="pointerBuffer" /> is not exactly 12 bytes.</exception>
    internal static void WriteCurrentPointerBlocking(IManifestPointerWriter writer, ReadOnlySpan<byte> pointerBuffer)
    {
        if (pointerBuffer.Length != Pointer.Size)
            throw new ArgumentException("Pointer buffer must be exactly 12 bytes.", nameof(pointerBuffer));

        writer.Write(pointerBuffer);
    }

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
    }

    /// <summary>Durably publishes a segment-roll manifest update (data file then pointer).</summary>
    /// <param name="targetPath">Path to a new numbered <c language="csharp">.bmqx</c> file.</param>
    /// <param name="encoded">Encoded manifest bytes.</param>
    /// <param name="pointerWriter">Reusable pointer writer for <c language="csharp">man-current</c>.</param>
    /// <param name="pointerBuffer">Exactly 12 encoded SQMC bytes.</param>
    internal static void WriteManifestRollBlocking(string targetPath, ReadOnlySpan<byte> encoded, IManifestPointerWriter pointerWriter, ReadOnlySpan<byte> pointerBuffer)
    {
        WriteManifestDataFileBlocking(targetPath, encoded);
        WriteCurrentPointerBlocking(pointerWriter, pointerBuffer);
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
