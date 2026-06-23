using System;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;

namespace Squirix.Server.Storage.Journaling.Platform.IoUring;

/// <summary>Raw Linux io_uring syscalls (no liburing).</summary>
[SupportedOSPlatform("linux")]
internal static partial class LinuxIoUringSyscalls
{
    internal const int MapShared = 0x01;
    internal const int ProtReadWrite = 0x03;
    internal const int SeekEnd = 2;

    internal const ulong OffSqRing = 0;
    internal const ulong OffCqRing = 0x8000000;
    internal const ulong OffSqes = 0x10000000;

    internal const uint EnterGetEvents = 1 << 0;

    internal const byte OpWrite = 23;
    internal const byte OpFsync = 3;

    internal const uint FsyncDatasync = 1;

    [LibraryImport("libc", EntryPoint = "mmap", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial nint Mmap(nint addr, nuint length, int prot, int flags, int fd, long offset);

    [LibraryImport("libc", EntryPoint = "munmap", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Munmap(nint addr, nuint length);

    [LibraryImport("libc", EntryPoint = "close", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Close(int fd);

    [LibraryImport("libc", EntryPoint = "lseek", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial long LSeek(int fd, long offset, int whence);

    [LibraryImport("libc", EntryPoint = "open", StringMarshalling = StringMarshalling.Utf8, SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int Open(string pathname, int flags, int mode);

    [LibraryImport("libc", EntryPoint = "ftruncate", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int FTruncate(int fd, long length);

    internal static int IoUringSetup(uint entries, ref IoUringParams parameters)
    {
        ArgumentOutOfRangeException.ThrowIfZero(entries);
        return SyscallIoUringSetup(425, entries, ref parameters);
    }

    internal static int IoUringEnter(int fd, uint toSubmit, uint minComplete, uint flags)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(fd);
        return SyscallIoUringEnter(426, fd, toSubmit, minComplete, flags, 0, 0);
    }

    [LibraryImport("libc", EntryPoint = "syscall", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial int SyscallIoUringSetup(int sysno, uint entries, ref IoUringParams parameters);

    [LibraryImport("libc", EntryPoint = "syscall", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    private static partial int SyscallIoUringEnter(int sysno, int fd, uint toSubmit, uint minComplete, uint flags, nint sigmask, nuint sigmaskSize);

    [StructLayout(LayoutKind.Sequential)]
    internal struct IoUringSqOffsets
    {
        internal uint Head;
        internal uint Tail;
        internal uint RingMask;
        internal uint RingEntries;
        internal uint Flags;
        internal uint Dropped;
        internal uint Array;
        internal uint Resv1;
        internal ulong Resv2;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct IoUringCqOffsets
    {
        internal uint Head;
        internal uint Tail;
        internal uint RingMask;
        internal uint RingEntries;
        internal uint Overflow;
        internal uint Cqes;
        internal uint Flags;
        internal uint Resv1;
        internal ulong Resv2;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct IoUringParams
    {
        internal uint SqEntries;
        internal uint CqEntries;
        internal uint Flags;
        internal uint SqThreadCpu;
        internal uint SqThreadIdle;
        internal uint Features;
        internal uint WqFd;
        internal uint Resv0;
        internal uint Resv1;
        internal uint Resv2;
        internal IoUringSqOffsets SqOff;
        internal IoUringCqOffsets CqOff;
    }

    [StructLayout(LayoutKind.Explicit, Size = 64)]
    internal struct IoUringSqe
    {
        [FieldOffset(0)]
        internal byte Opcode;

        [FieldOffset(1)]
        internal byte Flags;

        [FieldOffset(2)]
        internal ushort IoPrio;

        [FieldOffset(4)]
        internal int Fd;

        [FieldOffset(8)]
        internal ulong Off;

        [FieldOffset(16)]
        internal ulong Addr;

        [FieldOffset(24)]
        internal uint Len;

        [FieldOffset(28)]
        internal uint FsyncFlags;

        [FieldOffset(32)]
        internal ulong UserData;
    }

    [StructLayout(LayoutKind.Sequential)]
    internal struct IoUringCqe
    {
        internal ulong UserData;
        internal int Res;
        internal uint Flags;
    }
}
