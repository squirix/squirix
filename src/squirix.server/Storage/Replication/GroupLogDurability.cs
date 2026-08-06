using System;
using System.IO;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Storage.Replication;

/// <summary>Low-level <see cref="RandomAccess" /> file access for a replica-group log with write-through on Windows.</summary>
/// <remarks>
/// Methods are synchronous and intended to be invoked from a background durability worker. On Windows,
/// <see cref="FileOptions.WriteThrough" /> makes every write durable without an explicit disk flush;
/// <see cref="Flush" /> always calls <see cref="RandomAccess.FlushToDisk" /> on every platform to confirm durability.
/// </remarks>
internal sealed class GroupLogDurability : IDisposable
{
    private const string LogNotOpenMessage = "replica group log is not open.";

    private SafeFileHandle? _handle;

    /// <inheritdoc />
    public void Dispose()
    {
        _handle?.Dispose();
        _handle = null;
    }

    /// <summary>Opens (creating if needed) the log file and sizes it to <paramref name="length" />.</summary>
    /// <param name="path">The log file path.</param>
    /// <param name="length">The desired log length in bytes.</param>
    internal void Open(string path, long length)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        var options = FileOptions.None;
        if (OperatingSystem.IsWindows())
            options |= FileOptions.WriteThrough;

        _handle?.Dispose();
        _handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, options);
        RandomAccess.SetLength(_handle, length);
    }

    /// <summary>Truncates the log to <paramref name="length" /> bytes.</summary>
    /// <param name="length">The new log length in bytes.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Truncate(long length)
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.SetLength(handle, length);
    }

    /// <summary>Writes bytes at <paramref name="fileOffset" />.</summary>
    /// <param name="data">The bytes to write.</param>
    /// <param name="fileOffset">The byte offset at which to write.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Write(ReadOnlyMemory<byte> data, long fileOffset)
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.Write(handle, data.Span, fileOffset);
    }

    /// <summary>Flushes the log to stable storage.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Flush()
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.FlushToDisk(handle);
    }
}
