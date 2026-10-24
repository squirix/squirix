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
/// Network file systems without advisory locking, and the <c language="csharp">DOTNET_SYSTEM_IO_DISABLEFILELOCKING</c> switch, disable the protection.
/// </remarks>
internal sealed class DataDirectoryLock : IDisposable
{
    /// <summary>The name of the lock file inside the data directory.</summary>
    internal const string LockFileName = "node.lock";

    private const int LinuxWouldBlockErrno = 11;
    private const int SharingViolationWin32Code = 32;
    private const int LockViolationWin32Code = 33;
    private const int Win32CodeMask = 0xFFFF;
    private const int BsdWouldBlockErrno = 35;
    private readonly SafeFileHandle _handle;

    private DataDirectoryLock(SafeFileHandle handle)
    {
        _handle = handle;
    }

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
            return new DataDirectoryLock(File.OpenHandle(path, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.None));
        }
        catch (IOException ex) when (IsHeldByAnotherOwner(ex))
        {
            throw new InvalidOperationException(
                $"Data directory '{directory}' is in use by another node process: its lock file '{LockFileName}' is held. " +
                "Stop the other node or give this node its own data directory.",
                ex);
        }
    }

    private static bool IsHeldByAnotherOwner(IOException ex)
    {
        if (OperatingSystem.IsWindows())
        {
            var code = ex.HResult & Win32CodeMask;
            return code is SharingViolationWin32Code or LockViolationWin32Code;
        }

        return ex.HResult is LinuxWouldBlockErrno or BsdWouldBlockErrno;
    }
}
