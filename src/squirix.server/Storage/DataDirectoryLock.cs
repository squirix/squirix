using System;
using System.IO;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage;

/// <summary>Holds an exclusive lock on a persistence data directory for the lifetime of the owning node.</summary>
/// <remarks>
/// The lock is an exclusive-share handle on <see cref="LockFileName" />. The operating system refuses a second open of the file
/// from another process or from another handle of this process, and releases the lock when the handle closes or the process dies,
/// so a crashed node leaves nothing to clean up. The file is never deleted: removing it would race a new owner.
/// The container disposes the lock last. If an earlier component's dispose throws, the container stops disposing and the lock stays held
/// until the handle is finalized; that fails safe, because no second owner can take the directory meanwhile.
/// <see cref="IsEnforced" /> reports whether the operating system actually took the lock. Network file systems without advisory locking, and the <c language="csharp">DOTNET_SYSTEM_IO_DISABLEFILELOCKING</c> switch, disable the protection.
/// </remarks>
internal sealed class DataDirectoryLock : IDisposable
{
    /// <summary>The name of the lock file inside the data directory.</summary>
    internal const string LockFileName = "node.lock";

    private const int LinuxWouldBlockErrno = 11;
    private const int BsdWouldBlockErrno = 35;

    /// <summary>The HRESULT of the Win32 sharing violation, 0x80070020.</summary>
    private const int SharingViolationHResult = -2147024864;

    /// <summary>The HRESULT of the Win32 lock violation, 0x80070021.</summary>
    private const int LockViolationHResult = -2147024863;
    private readonly SafeFileHandle _handle;

    private DataDirectoryLock(SafeFileHandle handle, string directory, bool isEnforced)
    {
        _handle = handle;
        Directory = directory;
        IsEnforced = isEnforced;
    }

    /// <summary>Gets the normalized absolute path of the data directory.</summary>
    internal string Directory { get; }

    /// <summary>
    /// Gets a value indicating whether the operating system enforces the lock: a second open of the lock file was refused.
    /// <see langword="false" /> when file locking is disabled or unsupported by the file system, so another node on the directory goes undetected.
    /// </summary>
    internal bool IsEnforced { get; }

    /// <inheritdoc />
    public void Dispose() => _handle.Dispose();

    /// <summary>Creates the data directory when missing and takes the exclusive lock on it.</summary>
    /// <param name="dataDirectory">The data directory path.</param>
    /// <returns>The held lock; disposing it releases the directory.</returns>
    /// <exception cref="InvalidOperationException">Thrown when another node process or host already holds the directory.</exception>
    internal static DataDirectoryLock Acquire(string dataDirectory)
    {
        var directory = DirectoryEx.CreateDirectory(dataDirectory);
        var path = Path.Join(directory, LockFileName);
        try
        {
            var handle = File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None);
            try
            {
                return new DataDirectoryLock(handle, directory, !CanOpenAgain(path));
            }
            catch
            {
                handle.Dispose();
                throw;
            }
        }
        catch (IOException ex) when (IsHeldByAnotherOwner(ex))
        {
            throw new InvalidOperationException(
                $"Data directory '{directory}' is in use by another node (another process or another host in this process): its lock file '{LockFileName}' is held. " +
                "Stop the other node or give this node its own data directory.",
                ex);
        }
    }

    /// <summary>Tells whether the lock file opens a second time, which means the operating system took no lock on the first open.</summary>
    /// <param name="path">The lock file path.</param>
    /// <returns><see langword="true" /> when the second open succeeded; <see langword="false" /> when it was refused as a lock violation.</returns>
    private static bool CanOpenAgain(string path)
    {
        try
        {
            using var probe = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            return true;
        }
        catch (IOException ex) when (IsHeldByAnotherOwner(ex))
        {
            return false;
        }
    }

    private static bool IsHeldByAnotherOwner(IOException ex) =>
        (OperatingSystem.IsWindows() && ex.HResult is SharingViolationHResult or LockViolationHResult) ||
        (OperatingSystem.IsLinux() && ex.HResult == LinuxWouldBlockErrno) ||
        ((OperatingSystem.IsMacOS() || OperatingSystem.IsFreeBSD()) && ex.HResult == BsdWouldBlockErrno);
}
