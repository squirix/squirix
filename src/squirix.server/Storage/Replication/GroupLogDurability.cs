using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Low-level <see cref="RandomAccess" /> file access for a replica-group log with write-through on Windows.</summary>
/// <remarks>
/// Methods are synchronous and intended to be invoked from a background durability worker. On Windows,
/// <see cref="FileOptions.WriteThrough" /> does not replace a flush (it is honored only on SCSI/FC devices);
/// <see cref="Flush" /> always calls <see cref="RandomAccess.FlushToDisk" /> on every platform to flush the device write cache.
/// </remarks>
internal sealed class GroupLogDurability : IDisposable
{
    private const string LogNotOpenMessage = "replica group log is not open.";

    private SafeFileHandle? _handle;

    /// <inheritdoc />
    public void Dispose() => Reset();

    /// <summary>Opens a generation file that is not the live log and empties it, flushing the empty file and its directory entry.</summary>
    /// <param name="path">The generation file path.</param>
    /// <returns>The handle of the emptied file.</returns>
    internal static SafeFileHandle OpenStandby(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        var handle = OpenHandle(path);
        try
        {
            RandomAccess.SetLength(handle, 0);
            RandomAccess.FlushToDisk(handle);
            FileEx.FlushDirectoryEntry(path);
            return handle;
        }
        catch
        {
            handle.Dispose();
            throw;
        }
    }

    /// <summary>Writes the header of <paramref name="generation" /> at the start of a file whose frames are already flushed, then flushes it.</summary>
    /// <param name="handle">The handle of the generation file.</param>
    /// <param name="generation">The generation to seal, at least 1.</param>
    /// <remarks>This write is the commit point of a generation: a file without a valid header is never selected.</remarks>
    internal static void Seal(SafeFileHandle handle, ulong generation)
    {
        Span<byte> header = stackalloc byte[GroupLogSlots.HeaderSize];
        GroupLogSlots.WriteHeader(header, generation);
        RandomAccess.Write(handle, header, 0);
        RandomAccess.FlushToDisk(handle);
    }

    /// <summary>Flushes the log to stable storage.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Flush()
    {
        var handle = ThrowHelper.Required(_handle, LogNotOpenMessage);
        RandomAccess.FlushToDisk(handle);
    }

    /// <summary>Opens (creating if needed) the log file and sizes it to <paramref name="length" />.</summary>
    /// <param name="path">The log file path.</param>
    /// <param name="length">The desired log length in bytes.</param>
    internal void Open(string path, long length)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        Reset();
        var handle = OpenHandle(path);
        _ = Swap(handle);

        // The file may have just been created; its directory entry is flushed before the log is used.
        FileEx.FlushDirectoryEntry(path);
        RandomAccess.SetLength(handle, length);
    }

    /// <summary>Opens (creating if needed) the log file of a generation as its active file and seals it as that generation.</summary>
    /// <param name="path">The generation file path.</param>
    /// <param name="generation">The generation to seal, at least 1.</param>
    /// <remarks>Used when no log exists yet; the file holds only its header afterwards.</remarks>
    internal void Create(string path, ulong generation)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        Reset();
        var handle = OpenStandby(path);
        try
        {
            Seal(handle, generation);
        }
        catch
        {
            handle.Dispose();
            throw;
        }

        _ = Swap(handle);
    }

    /// <summary>Makes a sealed generation file the live log and empties the file it supersedes.</summary>
    /// <param name="handle">The handle of the sealed file; ownership moves to this instance.</param>
    /// <exception cref="IOException">Thrown when the superseded file cannot be emptied; the new file is already the live log by then.</exception>
    internal void Adopt(SafeFileHandle handle)
    {
        var previous = Swap(handle);
        if (previous == null)
            return;

        try
        {
            RandomAccess.SetLength(previous, 0);
            RandomAccess.FlushToDisk(previous);
        }
        finally
        {
            previous.Dispose();
        }
    }

    /// <summary>Truncates the log to <paramref name="length" /> bytes.</summary>
    /// <param name="length">The new log length in bytes.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Truncate(long length)
    {
        var handle = ThrowHelper.Required(_handle, LogNotOpenMessage);
        RandomAccess.SetLength(handle, length);
    }

    /// <summary>Writes bytes at <paramref name="fileOffset" /> and sizes the file to exactly <paramref name="fileOffset" /> plus the data length.</summary>
    /// <param name="data">The bytes to write.</param>
    /// <param name="fileOffset">The byte offset at which to write.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Write(ReadOnlyMemory<byte> data, long fileOffset)
    {
        var handle = ThrowHelper.Required(_handle, LogNotOpenMessage);
        RandomAccess.Write(handle, data.Span, fileOffset);

        // A failed appending can leave valid frames beyond the in-memory logical end; sizing the file to the
        // intended new end on every successful writing truncates any stale suffix so recovery never accepts it.
        RandomAccess.SetLength(handle, fileOffset + data.Length);
    }

    private static SafeFileHandle OpenHandle(string path)
    {
        var options = FileOptions.None;
        if (OperatingSystem.IsWindows())
            options |= FileOptions.WriteThrough;

        return File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, options);
    }

    private void Reset() => Swap(null)?.Dispose();

    /// <summary>Replaces the live handle; the only place that assigns it.</summary>
    /// <param name="next">The new live handle, or <see langword="null" /> when the log is closed.</param>
    /// <returns>The previous live handle, now owned by the caller.</returns>
    private SafeFileHandle? Swap(SafeFileHandle? next)
    {
        var previous = _handle;
        _handle = next;
        return previous;
    }
}
