using System;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Utils;

/// <summary>Native method declarations for the server's low-level file and directory operations.</summary>
internal static partial class NativeMethods
{
    /// <summary>Creates a manual-reset waitable timer, so a fired timer stays signalled until it is armed again.</summary>
    internal const uint CreateWaitableTimerManualReset = 0x1;

    /// <summary>Requests the high-resolution waitable timer (Windows 10 version 1803 and later).</summary>
    internal const uint CreateWaitableTimerHighResolution = 0x2;

    /// <summary>Requests full access to the waitable timer object.</summary>
    internal const uint TimerAllAccess = 0x1F0003;

    private const string LibcLibraryName = "libc";
    private const string DarwinSystemLibraryName = "libSystem.B.dylib";
    private const string Kernel32LibraryName = "kernel32.dll";
    private const string RestartManagerLibraryName = "rstrtmgr.dll";

    static NativeMethods()
    {
        NativeLibrary.SetDllImportResolver(typeof(NativeMethods).Assembly, ResolveLibc);
    }

    /// <summary>Opens a file descriptor through <c language="csharp">open(2)</c>.</summary>
    /// <param name="path">The NUL-terminated UTF-8 path bytes.</param>
    /// <param name="flags">The <c language="csharp">open(2)</c> flags. Creation flags such as <c language="csharp">O_CREAT</c> are not supported, because this declaration omits the variadic <c language="csharp">mode</c> argument.</param>
    /// <returns>The file descriptor, or a negative value on failure.</returns>
    /// <remarks>This import is valid on Unix platforms only.</remarks>
    [LibraryImport(LibcLibraryName, EntryPoint = "open", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.SafeDirectories)]
    internal static partial int OpenDirectoryDescriptor([In] byte[] path, int flags);

    /// <summary>Creates or opens a waitable timer through <c language="csharp">CreateWaitableTimerExW</c>.</summary>
    /// <param name="timerAttributes">Zero for default security attributes.</param>
    /// <param name="timerName">The optional timer name, or <see langword="null" /> for an unnamed timer.</param>
    /// <param name="flags">The creation flags.</param>
    /// <param name="desiredAccess">The requested access mask.</param>
    /// <returns>The timer handle; invalid when creation failed, for example when the flags are unsupported by the OS.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(Kernel32LibraryName, EntryPoint = "CreateWaitableTimerExW", SetLastError = true, StringMarshalling = StringMarshalling.Utf16)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    internal static partial SafeWaitHandle CreateWaitableTimerEx(nint timerAttributes, string? timerName, uint flags, uint desiredAccess);

    /// <summary>Arms a waitable timer through <c language="csharp">SetWaitableTimer</c>.</summary>
    /// <param name="timer">The timer handle.</param>
    /// <param name="dueTime">The due time in 100-nanosecond units; negative values are relative to now.</param>
    /// <param name="period">The period in milliseconds; zero for a one-shot timer.</param>
    /// <param name="completionRoutine">Zero: no completion routine is used.</param>
    /// <param name="completionRoutineArgument">Zero: no completion routine is used.</param>
    /// <param name="resume">Whether to resume a suspended system when the timer fires.</param>
    /// <returns><see langword="true" /> when the timer was armed.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(Kernel32LibraryName, EntryPoint = "SetWaitableTimer", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    internal static partial bool SetWaitableTimer(
        SafeWaitHandle timer,
        in long dueTime,
        int period,
        nint completionRoutine,
        nint completionRoutineArgument,
        [MarshalAs(UnmanagedType.Bool)] bool resume);

    /// <summary>Cancels an armed waitable timer through <c language="csharp">CancelWaitableTimer</c>.</summary>
    /// <param name="timer">The timer handle.</param>
    /// <returns><see langword="true" /> when the call succeeded.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(Kernel32LibraryName, EntryPoint = "CancelWaitableTimer", SetLastError = true)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    [return: MarshalAs(UnmanagedType.Bool)]
    internal static partial bool CancelWaitableTimer(SafeWaitHandle timer);

    /// <summary>Starts a Restart Manager session through <c language="csharp">RmStartSession</c>.</summary>
    /// <param name="sessionHandle">Receives the session handle.</param>
    /// <param name="sessionFlags">Reserved; zero.</param>
    /// <param name="sessionKey">A buffer of at least 66 bytes (33 UTF-16 code units) that receives the session key.</param>
    /// <returns>A Win32 error code; zero on success.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(RestartManagerLibraryName, EntryPoint = "RmStartSession")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    internal static partial int RestartManagerStartSession(out uint sessionHandle, uint sessionFlags, [Out] byte[] sessionKey);

    /// <summary>Registers files with a Restart Manager session through <c language="csharp">RmRegisterResources</c>.</summary>
    /// <param name="sessionHandle">The session handle.</param>
    /// <param name="fileCount">The number of entries in <paramref name="fileNames" />.</param>
    /// <param name="fileNames">The full file paths to register.</param>
    /// <param name="applicationCount">Zero: no applications are registered.</param>
    /// <param name="applications">Zero: no applications are registered.</param>
    /// <param name="serviceCount">Zero: no services are registered.</param>
    /// <param name="serviceNames">Zero: no services are registered.</param>
    /// <returns>A Win32 error code; zero on success.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(RestartManagerLibraryName, EntryPoint = "RmRegisterResources", StringMarshalling = StringMarshalling.Utf16)]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    internal static partial int RestartManagerRegisterFiles(uint sessionHandle, uint fileCount, string[] fileNames, uint applicationCount, nint applications, uint serviceCount, nint serviceNames);

    /// <summary>Lists the applications that hold the registered resources through <c language="csharp">RmGetList</c>.</summary>
    /// <param name="sessionHandle">The session handle.</param>
    /// <param name="needed">Receives the number of records required.</param>
    /// <param name="count">The capacity of <paramref name="processInfo" /> on input; the number of records written on output.</param>
    /// <param name="processInfo">The buffer of <c language="csharp">RM_PROCESS_INFO</c> records, 668 bytes each.</param>
    /// <param name="rebootReasons">Receives the reboot reasons bit mask.</param>
    /// <returns>A Win32 error code; zero on success and 234 when the buffer is too small.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(RestartManagerLibraryName, EntryPoint = "RmGetList")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    internal static partial int RestartManagerGetList(uint sessionHandle, out uint needed, ref uint count, [Out] byte[] processInfo, out uint rebootReasons);

    /// <summary>Ends a Restart Manager session through <c language="csharp">RmEndSession</c>.</summary>
    /// <param name="sessionHandle">The session handle.</param>
    /// <returns>A Win32 error code; zero on success.</returns>
    [SupportedOSPlatform("windows")]
    [LibraryImport(RestartManagerLibraryName, EntryPoint = "RmEndSession")]
    [DefaultDllImportSearchPaths(DllImportSearchPath.System32)]
    internal static partial int RestartManagerEndSession(uint sessionHandle);

    /// <summary>Resolves <c language="csharp">libc</c> imports on Apple platforms, where the BSD libc surface lives inside libSystem.</summary>
    /// <param name="libraryName">The library name requested by the P/Invoke declaration.</param>
    /// <param name="assembly">The assembly requesting the import.</param>
    /// <param name="searchPath">The default search path policy.</param>
    /// <returns>The loaded library handle, or <see cref="nint.Zero" /> to fall back to default probing.</returns>
    private static nint ResolveLibc(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        // macOS and Mac Catalyst ship no libc dylib to probe; Linux and FreeBSD keep their default libc probing.
        return string.Equals(libraryName, LibcLibraryName, StringComparison.Ordinal) switch
        {
            false => nint.Zero,
            true => NativeLibrary.TryLoad(DarwinSystemLibraryName, assembly, searchPath, out var handle) ? handle : nint.Zero,
        };
    }
}
