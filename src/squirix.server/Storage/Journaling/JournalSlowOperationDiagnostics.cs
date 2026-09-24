using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Allocation-free slow fsync and long mutation-gate hold warnings for the journal.</summary>
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

    private static void TraceLogFailure(Exception ex) => Trace.TraceError($"Journal slow-operation warning could not be logged: {ex}");

    private static long ElapsedMs(long startedTimestamp) => Convert.ToInt64(Stopwatch.GetElapsedTime(startedTimestamp).TotalMilliseconds);
}
