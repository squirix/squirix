using System;
using System.ComponentModel;
using System.IO;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Attributes;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Holds an exclusive byte-range lock on a file so that writes through other handles fail; Windows only.</summary>
[Mutable]
[SupportedOSPlatform("windows")]
internal sealed partial class ExclusiveRangeLock : IDisposable
{
    private const uint ExclusiveFlag = 0x2;
    private const uint FailImmediatelyFlag = 0x1;

    private readonly uint _length;
    private readonly SafeFileHandle _handle;

    internal ExclusiveRangeLock(string path, int length)
    {
        _length = uint.CreateChecked(length);
        _handle = File.OpenHandle(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        Overlapped overlapped = default;
        if (!NativeMethods.LockFileEx(_handle, ExclusiveFlag | FailImmediatelyFlag, 0, _length, 0, ref overlapped))
            throw new Win32Exception(Marshal.GetLastPInvokeError());
    }

    public void Dispose()
    {
        Overlapped overlapped = default;
        _ = NativeMethods.UnlockFileEx(_handle, 0, _length, 0, ref overlapped);
        _handle.Dispose();
    }

    [StructLayout(LayoutKind.Sequential)]
    private struct Overlapped
    {
        internal nuint Internal;
        internal nuint InternalHigh;
        internal uint Offset;
        internal uint OffsetHigh;
        internal nint Event;
    }

    private static partial class NativeMethods
    {
        [LibraryImport("kernel32.dll", SetLastError = true)]
        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool LockFileEx(SafeFileHandle file, uint flags, uint reserved, uint lengthLow, uint lengthHigh, ref Overlapped overlapped);

        [LibraryImport("kernel32.dll", SetLastError = true)]
        [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
        [return: MarshalAs(UnmanagedType.Bool)]
        internal static partial bool UnlockFileEx(SafeFileHandle file, uint reserved, uint lengthLow, uint lengthHigh, ref Overlapped overlapped);
    }
}
