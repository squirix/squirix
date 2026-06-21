using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Squirix.Server.Storage.Manifest.Binary;

/// <summary>Linux file helpers for binary manifest io_uring durability.</summary>
[SupportedOSPlatform("linux")]
internal static class LinuxManifestFile
{
    private const int OpenReadWrite = 0x0002;
    private const int OpenCreate = 0x0040;
    private const int OpenExclusive = 0x0080;
    private const int DefaultFileMode = 0x180;

    internal static int CreateNew(string path)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        var fd = LinuxManifestSyscalls.Open(path, OpenReadWrite | OpenCreate | OpenExclusive, DefaultFileMode);
        if (fd < 0)
            throw new IOException($"open failed for '{path}' with errno {Marshal.GetLastPInvokeError()}.");

        return fd;
    }

    internal static void Preallocate(int fd, long length)
    {
        if (length <= 0)
            return;

        if (LinuxManifestSyscalls.Fallocate(fd, 0, 0, length) is 0)
            return;

        if (LinuxManifestSyscalls.FTruncate(fd, length) is not 0)
            throw new IOException($"manifest preallocation failed with errno {Marshal.GetLastPInvokeError()}.");
    }

    internal static void Close(int fd)
    {
        if (fd < 0)
            return;

        _ = LinuxManifestSyscalls.Close(fd);
    }
}
