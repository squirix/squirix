using System.Runtime.InteropServices;
using System.Runtime.Versioning;
using Microsoft.Win32.SafeHandles;

namespace Squirix.Server.Threading;

/// <summary>Native declarations for the Windows waitable timer primitive.</summary>
internal static partial class NativeMethods
{
    /// <summary>Creates a manual-reset waitable timer, so a fired timer stays signalled until it is armed again.</summary>
    internal const uint CreateWaitableTimerManualReset = 0x1;

    /// <summary>Requests the high-resolution waitable timer (Windows 10 version 1803 and later).</summary>
    internal const uint CreateWaitableTimerHighResolution = 0x2;

    /// <summary>Requests full access to the waitable timer object.</summary>
    internal const uint TimerAllAccess = 0x1F0003;

    private const string Kernel32LibraryName = "kernel32.dll";

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
}
