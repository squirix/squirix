using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Low-level <see cref="RandomAccess" /> file access for a replica-group log with write-through on Windows.</summary>
/// <remarks>
/// Methods are synchronous and intended to be invoked from a background durability worker. On Windows,
/// <see cref="FileOptions.WriteThrough" /> makes every writing durable without an explicit disk flush;
/// <see cref="Flush" /> always calls <see cref="RandomAccess.FlushToDisk" /> on every platform to confirm durability.
/// </remarks>
internal sealed class GroupLogDurability : IDisposable
{
    private const string LogNotOpenMessage = "replica group log is not open.";

    private SafeFileHandle? _handle;

    /// <inheritdoc />
    public void Dispose() => Reset();

    /// <summary>Flushes the log to stable storage.</summary>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Flush()
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.FlushToDisk(handle);
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

        Reset();
        _handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read, options);
        RandomAccess.SetLength(_handle, length);
    }

    /// <summary>Atomically publishes a fully flushed replacement and reopens the durable handle.</summary>
    /// <param name="tempPath">The fully written temporary log path.</param>
    /// <param name="finalPath">The live log path.</param>
    /// <param name="length">The exact length of the replacement log.</param>
    /// <exception cref="InvalidOperationException">Thrown when the replacement path has no containing directory.</exception>
    /// <exception cref="AggregateException">Thrown when reopening the published log fails during recovery.</exception>
    /// <remarks>
    /// The current handle is closed before publication. When publication fails and no published file exists, the
    /// instance is left with no open handle; the caller must fail readiness instead of reusing it. When publication
    /// fails but a log still exists at <paramref name="finalPath" />, the instance is reopened on that file, and the
    /// original failure is rethrown — for a publication failure this is the pre-replacement log, while a directory-flush
    /// failure reopens the newly published one; either way the caller must still fail readiness, because the
    /// compaction result was not confirmed durable.
    /// </remarks>
    internal void Replace(string tempPath, string finalPath, long length)
    {
        // A pure argument error must leave the live handle attached: validating after Dispose would strand the
        // instance without a handle even though no file operation was attempted. The temp file is removed here, so
        // the refused replacement leaves no debris, matching the publication-failure cleanup contract.
        var validatedTemp = FilePathValidator.ResolveValidatedFilePath(tempPath);
        if (string.IsNullOrEmpty(Path.GetDirectoryName(finalPath)))
        {
            _ = FileEx.TryDeleteFile(validatedTemp);
            throw new InvalidOperationException($"Replica group log path '{finalPath}' has no containing directory.");
        }

        var validatedFinal = FilePathValidator.ResolveValidatedFilePath(finalPath);

        Reset();
        var published = false;
        try
        {
            published = FileEx.PublishFile(validatedTemp, validatedFinal);
            Open(validatedFinal, length);
        }
        catch (Exception failure) when (File.Exists(validatedFinal))
        {
            // Open assigns the handle before sizing the file; if Open fails after assignment, the handle would
            // otherwise stay open at a length that was never set. Reset it before attempting recovery.
            Reset();
            try
            {
                Open(validatedFinal, new FileInfo(validatedFinal).Length);
            }
            catch (Exception recoveryFailure)
            {
                // Open assigns the handle before sizing the file; a failed recovery reopened would otherwise leave
                // it attached, so reset it before rethrowing and let later Flush, Write, and Truncate calls
                // observe a closed log. Every recovery failure type is caught, so the aggregate always carries the
                // original cause too; an unfiltered rethrow would replace it and lose the reason Replace failed.
                Reset();
                throw new AggregateException("Failed to reopen the published replica group log after replacing it.", failure, recoveryFailure);
            }

            throw;
        }
        finally
        {
            if (!published)
                _ = FileEx.TryDeleteFile(validatedTemp);
        }
    }

    /// <summary>Truncates the log to <paramref name="length" /> bytes.</summary>
    /// <param name="length">The new log length in bytes.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Truncate(long length)
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.SetLength(handle, length);
    }

    /// <summary>Writes bytes at <paramref name="fileOffset" /> and sizes the file to exactly <paramref name="fileOffset" /> plus the data length.</summary>
    /// <param name="data">The bytes to write.</param>
    /// <param name="fileOffset">The byte offset at which to write.</param>
    /// <exception cref="InvalidOperationException">Thrown when the log handle is not open.</exception>
    internal void Write(ReadOnlyMemory<byte> data, long fileOffset)
    {
        var handle = _handle ?? throw new InvalidOperationException(LogNotOpenMessage);
        RandomAccess.Write(handle, data.Span, fileOffset);

        // A failed appending can leave valid frames beyond the in-memory logical end; sizing the file to the
        // intended new end on every successful writing truncates any stale suffix so recovery never accepts it.
        RandomAccess.SetLength(handle, fileOffset + data.Length);
    }

    private void Reset()
    {
        _handle?.Dispose();
        _handle = null;
    }
}
