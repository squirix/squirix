using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Allocation-free slow fsync, long mutation-gate hold and stalled-wait warnings for the journal.</summary>
internal static class JournalSlowOperationDiagnostics
{
    internal const int WarningThresholdMs = 1000;

    /// <summary>Warns when an fsync started at <paramref name="startedTimestamp" /> exceeded the threshold; never throws.</summary>
    /// <param name="logger">The journal logger.</param>
    /// <param name="startedTimestamp">The <see cref="Stopwatch.GetTimestamp" /> value taken before the fsync.</param>
    internal static void ReportFsync(ILogger logger, long startedTimestamp)
    {
        var elapsedMs = ElapsedMs(startedTimestamp);
        if (elapsedMs < WarningThresholdMs)
            return;

        try
        {
            LogManager.JournalFsyncSlow(logger, elapsedMs);
        }
#pragma warning disable CA1031 // Diagnostics only: a faulty log sink must not fail the journal thread after a successful fsync.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            TraceLogFailure(ex);
        }
    }

    /// <summary>Warns when a mutation gate held since <paramref name="acquiredTimestamp" /> exceeded the threshold; never throws.</summary>
    /// <param name="logger">The journal coordinator logger.</param>
    /// <param name="acquiredTimestamp">The <see cref="Stopwatch.GetTimestamp" /> value taken after the gate was acquired.</param>
    /// <param name="holder">The gate holder site.</param>
    internal static void ReportMutationGateHold(ILogger logger, long acquiredTimestamp, string holder)
    {
        var heldMs = ElapsedMs(acquiredTimestamp);
        if (heldMs < WarningThresholdMs)
            return;

        try
        {
            LogManager.JournalMutationGateHeldLong(logger, heldMs, holder);
        }
#pragma warning disable CA1031 // Diagnostics only: a faulty log sink must not mask the gate holder's own outcome.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            TraceLogFailure(ex);
        }
    }

    /// <summary>
    /// Warns that a wait on the journal was canceled while the journal was stalled (a segment I/O call or a mutation-gate
    /// hold at or beyond the threshold), so the holder is visible at the moment a commit budget expires; never throws.
    /// </summary>
    /// <param name="logger">The journal coordinator logger.</param>
    /// <param name="waitingFor">What the canceled wait was waiting for.</param>
    /// <param name="ioOperation">The segment I/O operation in progress on the journal thread, if any.</param>
    /// <param name="ioMs">How long that operation has been running, or zero when none is.</param>
    /// <param name="gateHolder">The current mutation-gate holder site, if the gate is held.</param>
    /// <param name="gateHeldMs">How long the gate has been held, or zero when it is free.</param>
    internal static void ReportWaitCanceled(ILogger logger, string waitingFor, string? ioOperation, long ioMs, string? gateHolder, long gateHeldMs)
    {
        if (ioMs < WarningThresholdMs && gateHeldMs < WarningThresholdMs)
            return;

        try
        {
            LogManager.JournalWaitCanceledWhileStalled(logger, waitingFor, ioOperation ?? "none", ioMs, gateHolder ?? "none", gateHeldMs);
        }
#pragma warning disable CA1031 // Diagnostics only: a faulty log sink must not mask the cancellation of the wait.
        catch (Exception ex)
#pragma warning restore CA1031
        {
            TraceLogFailure(ex);
        }
    }

    /// <summary>Returns the milliseconds elapsed since <paramref name="timestamp" />, or zero when it is zero (not in progress).</summary>
    /// <param name="timestamp">A <see cref="Stopwatch.GetTimestamp" /> value, or zero.</param>
    /// <returns>The elapsed milliseconds, or zero.</returns>
    internal static long ElapsedMsSince(long timestamp) => timestamp == 0 ? 0 : ElapsedMs(timestamp);

    private static void TraceLogFailure(Exception ex) => Trace.TraceError($"Journal slow-operation warning could not be logged: {ex}");

    private static long ElapsedMs(long startedTimestamp) => Convert.ToInt64(Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds);
}
