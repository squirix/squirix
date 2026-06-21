using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Squirix.Server.Storage.Manifest;

/// <summary>Minimal Linux libc imports for Manifest io_uring durability.</summary>
[SupportedOSPlatform("linux")]
internal static partial class LinuxManifestSyscalls
{
    [LibraryImport("libc", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Open(string pathname, int flags, int mode);

    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Close(int fd);

    [LibraryImport("libc", EntryPoint = "fallocate", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Fallocate(int fd, int mode, long offset, long len);

    [LibraryImport("libc", EntryPoint = "ftruncate", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int FTruncate(int fd, long length);
}
