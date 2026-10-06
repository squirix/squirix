using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Utils;

/// <summary>File helpers for durable publication, discovery, and best-effort deletion.</summary>
internal static class FileEx
{
    /// <summary>Maximum number of attempts <see cref="PublishFile" /> makes under a transient Windows sharing failure.</summary>
    internal const int PublishAttempts = 5;

    private const int DarwinCloseOnExec = 0x1000000;

    private const int FreeBsdCloseOnExec = 0x00100000;

    /// <summary>O_CLOEXEC flag values per supported Unix ABI (stable kernel constants from fcntl.h), OR'd with O_RDONLY (0).</summary>
    private const int LinuxCloseOnExec = 0x80000;

    /// <summary>Gets the delay between <see cref="PublishFile" /> attempts under a transient Windows sharing failure.</summary>
    internal static TimeSpan PublishRetryDelay { get; } = TimeSpan.FromMilliseconds(50);

    internal static string? FindFile(ReadOnlySpan<string> paths)
    {
        var cwd = Directory.GetCurrentDirectory();
        foreach (var name in paths)
        {
            var p = PathEx.Combine(cwd, name);
            if (File.Exists(p))
                return p;
        }

        var baseDir = AppContext.BaseDirectory;
        foreach (var name in paths)
        {
            var p = PathEx.Combine(baseDir, name);
            if (File.Exists(p))
                return p;
        }

        return null;
    }

    /// <summary>
    /// Flushes the parent directory of <paramref name="filePath" /> so a recent directory-entry change
    /// (create, rename, or delete) survives a crash.
    /// </summary>
    /// <param name="filePath">Path of the file whose parent directory must be flushed.</param>
    /// <remarks>
    /// On Unix, opens the parent directory and calls <c language="csharp">fsync(2)</c> to guarantee directory-entry durability.
    /// On Windows, this is a no-op: Microsoft does not document <c language="csharp">FlushFileBuffers</c> as a
    /// directory-entry durability primitive, and NTFS metadata journaling provides implicit directory-entry
    /// durability without an explicit flush. This matches upstream SQLite behavior (<c language="csharp">os_win.c</c>),
    /// which never fsyncs directories on Windows.
    /// </remarks>
    /// <exception cref="IOException">Thrown when the Unix directory descriptor cannot be opened or flushed.</exception>
    internal static void FlushDirectoryEntry(string filePath)
    {
        if (OperatingSystem.IsWindows())
            return;

        var directory = Path.GetDirectoryName(filePath);
        if (string.IsNullOrEmpty(directory))
            return;

        using var handle = OpenDirectoryForFlush(directory);
        RandomAccess.FlushToDisk(handle);
    }

    /// <summary>Publishes a temp file as the final durable file, replacing an existing destination when present.</summary>
    /// <param name="tempPath">Path to the fully written temp file.</param>
    /// <param name="finalPath">Destination path that should reference <paramref name="tempPath" /> after completion.</param>
    /// <param name="timeProvider">Clock used to wait between attempts.</param>
    /// <param name="backupPath">Optional backup path used when <paramref name="finalPath" /> already exists.</param>
    /// <param name="ignoreMetadataErrors">
    /// When <see langword="true" />, metadata differences between source and destination are ignored during
    /// <see cref="File.Replace(string, string, string?, bool)" />.
    /// </param>
    /// <returns>Always <see langword="true" /> when publication succeeds; failures throw.</returns>
    /// <remarks>
    /// On Windows, a destination that is transiently held open (for example by an on-close scanner or a lingering handle)
    /// makes the replace fail with an <see cref="IOException" /> whose HResult is <c language="csharp">0x80070020</c> (sharing violation),
    /// <c language="csharp">0x80070497</c> (unable to remove the replaced file), or <c language="csharp">0x80070498</c> (unable to move the replacement file).
    /// Only these failures are retried, up to <see cref="PublishAttempts" /> attempts spaced by <see cref="PublishRetryDelay" />;
    /// the exception of the last attempt propagates unchanged. Every other failure surfaces on the first attempt.
    /// The temp file is never deleted by this method, so the caller keeps ownership of it on failure.
    /// </remarks>
    internal static bool PublishFile(
        string tempPath,
        string finalPath,
        TimeProvider timeProvider,
        string? backupPath = null,
        bool ignoreMetadataErrors = false)
    {
        var validatedTemp = FilePathValidator.ResolveValidatedFilePath(tempPath);
        var validatedFinal = FilePathValidator.ResolveValidatedFilePath(finalPath);
        var validatedBackup = backupPath == null ? null : FilePathValidator.ResolveValidatedFilePath(backupPath);

        var attempt = 1;
        while (true)
        {
            try
            {
                if (File.Exists(validatedFinal))
                    File.Replace(validatedTemp, validatedFinal, validatedBackup, ignoreMetadataErrors);
                else
                    File.Move(validatedTemp, validatedFinal);

                break;
            }
            catch (IOException ex) when (attempt < PublishAttempts && IsTransientWindowsSharingFailure(ex))
            {
                attempt++;
                WaitBeforeRetry(timeProvider, PublishRetryDelay);
            }
        }

        // Temp, final, and backup always share a directory; flushing the destination's parent directory is enough to make the rename's directory entry durable.
        FlushDirectoryEntry(validatedFinal);
        return true;
    }

    /// <summary>Attempts to delete a file at the given <paramref name="path" />.</summary>
    /// <param name="path">
    /// Absolute or relative path to the file to delete. If <see langword="null" />, empty, or whitespace-only,
    /// the method succeeds without performing any action. If the string contains any character from
    /// <see cref="Path.GetInvalidPathChars" />, the method succeeds without calling file APIs.
    /// </param>
    /// <returns>
    /// <see langword="true" /> when the path is skipped as invalid, the file did not exist, or deletion completed;
    /// <see langword="false" /> when deletion was attempted but failed.
    /// </returns>
    /// <remarks>
    /// Best-effort cleanup helper for teardown paths where callers ignore failures.
    /// For strict deletion semantics, use <see cref="File.Delete(string)" /> directly.
    /// </remarks>
    internal static bool TryDeleteFile(string? path)
    {
        if (string.IsNullOrWhiteSpace(path) || path.IndexOfAny(Path.GetInvalidPathChars()) >= 0)
            return true;

        try
        {
            return TryDeleteExistingFile(FilePathValidator.ResolveValidatedFilePath(path));
        }
        catch (ArgumentException)
        {
            return true;
        }
    }

    /// <summary>Returns the platform-specific <c language="csharp">O_CLOEXEC</c> flag so the directory descriptor is closed on exec.</summary>
    /// <remarks>
    /// Unknown Unix platforms return <c language="csharp">0</c> (no close-on-exec), preserving the previous behavior rather than
    /// risking an invalid flag. This path only runs on Unix; <see cref="FlushDirectoryEntry" /> no-ops on Windows.
    /// </remarks>
    private static int CloseOnExecFlag()
    {
        var isApple = OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst();
        return true switch
        {
            _ when OperatingSystem.IsLinux() => LinuxCloseOnExec,
            _ when isApple => DarwinCloseOnExec,
            _ when OperatingSystem.IsFreeBSD() => FreeBsdCloseOnExec,
            _ => 0,
        };
    }

    private static bool IsTransientWindowsSharingFailure(IOException exception)
    {
        // 0x80070020, 0x80070497, 0x80070498 as signed HResults.
        const int sharingViolation = -2147024864;
        const int unableToRemoveReplaced = -2147023721;
        const int unableToMoveReplacement = -2147023720;

        return OperatingSystem.IsWindows()
               && exception.HResult is sharingViolation or unableToRemoveReplaced or unableToMoveReplacement;
    }

    private static SafeFileHandle OpenDirectoryForFlush(string directory)
    {
        // EINTR (interrupted system call) is 4 on Linux, macOS, and the *BSD family.
        // This path only runs on Unix, where open(2) can be interrupted by a signal.
        const int eintr = 4;
        var pathBytes = Encoding.UTF8.GetBytes(directory + "\0");

        for (var attempt = 0; attempt < 5; attempt++)
        {
            var descriptor = NativeMethods.OpenDirectoryDescriptor(pathBytes, CloseOnExecFlag());
            if (descriptor >= 0)
                return new SafeFileHandle(new nint(descriptor), true);

            // A system call interrupted by a signal must be retried; any other failure is surfaced as-is via the existing IOException below.
            if (Marshal.GetLastPInvokeError() != eintr)
                break;
        }

        throw new IOException($"Failed to open directory '{directory}' for flushing; errno={Marshal.GetLastPInvokeError()}.");
    }

    private static bool TryDeleteExistingFile(string validatedPath)
    {
        try
        {
            if (!File.Exists(validatedPath))
                return true;

            File.Delete(validatedPath);
            return true;
        }
        catch (Exception ex) when (ex is IOException or UnauthorizedAccessException)
        {
            return false;
        }
    }

    private static void WaitBeforeRetry(TimeProvider timeProvider, TimeSpan delay)
    {
        using var fired = new ManualResetEventSlim(false);
        var timer = timeProvider.CreateTimer(SignalGate, fired, delay, Timeout.InfiniteTimeSpan);
        try
        {
            fired.Wait(CancellationToken.None);
        }
        finally
        {
            timer.Dispose();
        }
    }

    private static void SignalGate(object? state)
    {
        if (state is ManualResetEventSlim gate)
            gate.Set();
    }
}
