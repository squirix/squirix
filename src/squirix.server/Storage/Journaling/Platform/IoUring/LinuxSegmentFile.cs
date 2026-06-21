using System;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Squirix.Server.Storage.Journaling.Platform.IoUring;

/// <summary>Linux segment file helpers for the io_uring writer path.</summary>
[SupportedOSPlatform("linux")]
internal static class LinuxSegmentFile
{
    private const int OpenReadWrite = 0x0002;
    private const int OpenCreate = 0x0040;
    private const int OpenTruncate = 0x0200;
    private const int DefaultFileMode = 0x180;

    internal static int Open(string path, bool append)
    {
        ArgumentException.ThrowIfNullOrEmpty(path);
        var flags = OpenReadWrite | OpenCreate;
        if (!append)
            flags |= OpenTruncate;

        var fd = LinuxIoUringSyscalls.Open(path, flags, DefaultFileMode);
        if (fd < 0)
            throw new IOException($"open failed for '{path}' with errno {Marshal.GetLastPInvokeError()}.");

        return fd;
    }

    internal static long GetLength(int fd)
    {
        var length = LinuxIoUringSyscalls.LSeek(fd, 0, LinuxIoUringSyscalls.SeekEnd);
        if (length < 0)
            throw new IOException($"lseek failed with errno {Marshal.GetLastPInvokeError()}.");

        return length;
    }

    internal static void Truncate(int fd, long length)
    {
        if (LinuxIoUringSyscalls.FTruncate(fd, length) is not 0)
            throw new IOException($"ftruncate failed with errno {Marshal.GetLastPInvokeError()}.");
    }

    internal static void Close(int fd)
    {
        if (fd < 0)
            return;

        _ = LinuxIoUringSyscalls.Close(fd);
    }
}
