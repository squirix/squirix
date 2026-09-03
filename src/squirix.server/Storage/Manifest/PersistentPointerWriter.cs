using System;
using System.IO;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Durable writer for the fixed-size SQMC <c language="csharp">man-current</c> pointer.</summary>
/// <remarks>
/// Each write is staged to a fixed temporary file (<c language="csharp">man-current.next</c>) in the same directory and then
/// atomically replaced into <c language="csharp">man-current</c>. A concurrent reader can therefore only ever observe a fully
/// written, valid pointer (or the previous valid one) — never a torn or zeroed file left mid-update. The stage
/// file is opened with <see cref="FileMode.Create" />, so a leftover from a previous failed write is safely
/// overwritten; <see cref="Write" /> is only ever called serially by the manifest roll worker.
/// </remarks>
[Immutable]
internal sealed class PersistentPointerWriter : IManifestPointerWriter
{
    private readonly string _path;
    private readonly string _tempPath;

    internal PersistentPointerWriter(string path)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(path);

        var directory = ThrowHelper.Required(Path.GetDirectoryName(path), $"Pointer path has no directory: {path}");
        _path = path;
        _tempPath = PathEx.Combine(directory, "man-current.next");
    }

    public void Write(ReadOnlySpan<byte> buffer)
    {
        if (buffer.Length != Pointer.Size)
            throw new ArgumentException("Pointer buffer must be exactly 12 bytes.", nameof(buffer));

        try
        {
            using (var handle = File.OpenHandle(_tempPath, FileMode.Create, FileAccess.Write, FileShare.None, FileDurability.GetPointerFileOptions()))
            {
                RandomAccess.Write(handle, buffer, 0);
                FileDurability.FlushPointerIfNeeded(handle);
            }

            File.Move(_tempPath, _path, true);
            FileEx.FlushDirectoryEntry(_path);
        }
        catch
        {
            _ = FileEx.TryDeleteFile(_tempPath);
            throw;
        }
    }
}
