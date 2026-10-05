using System;
using System.Threading;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Utils;

namespace Squirix.Server.Threading;

/// <summary>
/// Waits for a work signal or a relative deadline. On Windows 10 version 1803 and later the deadline is a high-resolution waitable
/// timer, so a short wait ends close to the requested time instead of rounding up to the system clock tick (about 15.6 ms). Where
/// the timer is unavailable the wait falls back to the work signal's own timeout. Not thread-safe: every member except construction
/// must be called from one thread (the journal thread), and <see cref="Dispose" /> only after that thread stopped.
/// </summary>
internal sealed class HighResolutionDeadlineWait : IDisposable
{
    /// <summary>Slack added to the handle wait so a timer that never fires cannot hold the thread past this bound.</summary>
    private const int BackstopMs = 32;

    private const int TimerHandleIndex = 1;
    private const long TicksPerMillisecond = 10_000;
    private readonly AutoResetEvent _workSignal;
    private bool _armed;
    private int _disposed;
    private WaitHandle[]? _handles;
    private DeadlineTimerHandle? _timer;

    internal HighResolutionDeadlineWait(AutoResetEvent workSignal, bool useHighResolutionTimer)
    {
        ArgumentNullException.ThrowIfNull(workSignal);
        _workSignal = workSignal;
        IsHighResolutionUnavailable = !useHighResolutionTimer;
    }

    /// <summary>Gets a value indicating whether a finite wait currently uses the high-resolution timer.</summary>
    internal bool IsHighResolutionActive => _timer != null && !IsHighResolutionUnavailable;

    /// <summary>Gets a value indicating whether the high-resolution timer is not in use: disabled by the caller, or its creation failed.</summary>
    internal bool IsHighResolutionUnavailable { get; private set; }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        _armed = false;
        _handles = null;
        _timer?.Dispose();
        _timer = null;
    }

    /// <summary>Blocks until the work signal fires or the relative deadline passes. May return early or spuriously; the caller re-evaluates.</summary>
    /// <param name="timeoutMs">A positive number of milliseconds, or <see cref="Timeout.Infinite" />.</param>
    internal void Wait(int timeoutMs)
    {
        if (timeoutMs == Timeout.Infinite)
        {
            // A previously armed timer is not in this wait set; cancelling keeps it from sitting signalled
            // until the next finite wait re-arms it.
            CancelArmedTimer();
            _ = _workSignal.WaitOne();
            return;
        }

        if (TryArmTimer(timeoutMs))
        {
            var backstopMs = timeoutMs > int.MaxValue - BackstopMs ? int.MaxValue - 1 : timeoutMs + BackstopMs;
            if (WaitHandle.WaitAny(_handles!, backstopMs) == TimerHandleIndex)
                _armed = false;

            return;
        }

        _ = _workSignal.WaitOne(timeoutMs);
    }

    private void CancelArmedTimer()
    {
        if (!_armed || _timer == null)
            return;

        _armed = false;
        if (OperatingSystem.IsWindows())
            _ = NativeMethods.CancelWaitableTimer(_timer.SafeWaitHandle);
    }

    private bool TryArmTimer(int timeoutMs)
    {
        if (IsHighResolutionUnavailable || Volatile.Read(ref _disposed) == 1 || !OperatingSystem.IsWindows())
            return false;

        if (_timer == null && !TryCreateTimer())
            return false;

        var dueTime = -timeoutMs * TicksPerMillisecond;
        if (!NativeMethods.SetWaitableTimer(_timer!.SafeWaitHandle, in dueTime, 0, nint.Zero, nint.Zero, false))
            return false;

        _armed = true;
        return true;
    }

    private bool TryCreateTimer()
    {
        if (!OperatingSystem.IsWindows())
            return false;

        var handle = NativeMethods.CreateWaitableTimerEx(
            nint.Zero,
            null,
            NativeMethods.CreateWaitableTimerManualReset | NativeMethods.CreateWaitableTimerHighResolution,
            NativeMethods.TimerAllAccess);
        if (handle.IsInvalid)
        {
            handle.Dispose();
            IsHighResolutionUnavailable = true;
            return false;
        }

        _timer = new DeadlineTimerHandle(handle);
        _handles = [_workSignal, _timer];
        return true;
    }

    /// <summary>Exposes a native waitable timer handle as a <see cref="WaitHandle" /> so it can join a multi-handle wait.</summary>
    private sealed class DeadlineTimerHandle : WaitHandle
    {
        internal DeadlineTimerHandle(SafeWaitHandle handle)
        {
            SafeWaitHandle = handle;
        }
    }
}
