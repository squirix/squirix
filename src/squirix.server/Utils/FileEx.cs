using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Utils;

/// <summary>File helpers for durable publication, discovery, and best-effort deletion.</summary>
internal static partial class FileEx
{
    private const int DarwinCloseOnExec = 0x1000000;
    private const int FreeBsdCloseOnExec = 0x00100000;

    /// <summary>O_CLOEXEC flag values per supported Unix ABI (stable kernel constants from fcntl.h), OR'd with O_RDONLY (0).</summary>
    private const int LinuxCloseOnExec = 0x80000;

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
    /// (create, rename, or delete) survives a crash. A no-op on Windows.
    /// </summary>
    /// <param name="filePath">Path of the file whose parent directory must be flushed.</param>
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
    /// <param name="backupPath">Optional backup path used when <paramref name="finalPath" /> already exists.</param>
    /// <param name="ignoreMetadataErrors">
    /// When <see langword="true" />, metadata differences between source and destination are ignored during
    /// <see cref="File.Replace(string, string, string?, bool)" />.
    /// </param>
    internal static void PublishFile(string tempPath, string finalPath, string? backupPath = null, bool ignoreMetadataErrors = false)
    {
        var validatedTemp = FilePathValidator.ResolveValidatedFilePath(tempPath);
        var validatedFinal = FilePathValidator.ResolveValidatedFilePath(finalPath);
        var validatedBackup = backupPath == null ? null : FilePathValidator.ResolveValidatedFilePath(backupPath);
        if (File.Exists(validatedFinal))
            File.Replace(validatedTemp, validatedFinal, validatedBackup, ignoreMetadataErrors);
        else
            File.Move(validatedTemp, validatedFinal);

        FlushDirectoryEntry(validatedFinal);
    }

    /// <summary>
    /// Attempts to delete a file at the given <paramref name="path" />.
    /// </summary>
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
        if (OperatingSystem.IsLinux())
            return LinuxCloseOnExec;
        if (OperatingSystem.IsMacOS() || OperatingSystem.IsMacCatalyst())
            return DarwinCloseOnExec;
        if (OperatingSystem.IsFreeBSD())
            return FreeBsdCloseOnExec;
        return 0;
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
                return new SafeFileHandle(new IntPtr(descriptor), true);

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
        catch (IOException)
        {
            return false;
        }
        catch (UnauthorizedAccessException)
        {
            return false;
        }
    }

    /// <summary>Platforms invoke methods for unmanaged file-system calls used by <see cref="FileEx" />.</summary>
    /// <remarks>Declared as a dedicated <c language="csharp">NativeMethods</c> class per NDepend ND2401.</remarks>
    private static partial class NativeMethods
    {
        [LibraryImport("libc", EntryPoint = "open", SetLastError = true)]
        [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
        internal static partial int OpenDirectoryDescriptor([In] byte[] path, int flags);
    }
}
