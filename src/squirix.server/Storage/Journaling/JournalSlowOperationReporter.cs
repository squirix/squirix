using System;
using System.Diagnostics;
using Microsoft.Extensions.Logging;
using Squirix.Server.Threading;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Allocation-free slow fsync, long mutation-gate hold and stalled-wait warnings for the journal.</summary>
internal sealed class JournalSlowOperationReporter
{
    internal const int WarningThresholdMs = 1000;

    private readonly ILogger _logger;
    private readonly TimeProvider _timeProvider;

    /// <summary>Initializes a new instance of the <see cref="JournalSlowOperationReporter" /> class.</summary>
    /// <param name="logger">The logger the warnings go to.</param>
    /// <param name="timeProvider">The clock every timestamp handed to this reporter is taken from and measured with.</param>
    internal JournalSlowOperationReporter(ILogger logger, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(logger);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _logger = logger;
        _timeProvider = timeProvider;
    }

    /// <summary>Returns the milliseconds elapsed since <paramref name="timestamp" />, or zero when it is zero (not in progress).</summary>
    /// <param name="timestamp">A <see cref="GetTimestamp" /> value, or zero.</param>
    /// <returns>The elapsed milliseconds, or zero.</returns>
    internal long ElapsedMsSince(long timestamp) => timestamp == 0 ? 0 : ElapsedMs(timestamp);

    /// <summary>Reads the current timestamp of the reporter clock.</summary>
    /// <returns>The timestamp to hand back to this reporter.</returns>
    internal long GetTimestamp() => _timeProvider.GetTimestamp();

    /// <summary>Warns when an fsync started at <paramref name="startedTimestamp" /> exceeded the threshold; never throws.</summary>
    /// <param name="startedTimestamp">The <see cref="GetTimestamp" /> value taken before the fsync.</param>
    internal void ReportFsync(long startedTimestamp)
    {
        var elapsedMs = ElapsedMs(startedTimestamp);
        if (elapsedMs < WarningThresholdMs)
            return;

        // Diagnostics only: a faulty log sink must not fail the journal thread after a successful fsync.
        TraceLogFailure(Isolated.Run((Logger: _logger, ElapsedMs: elapsedMs), static state => ServerLog.JournalFsyncSlow(state.Logger, state.ElapsedMs)));
    }

    /// <summary>Warns when a mutation gate held since <paramref name="acquiredTimestamp" /> exceeded the threshold; never throws.</summary>
    /// <param name="acquiredTimestamp">The <see cref="GetTimestamp" /> value taken after the gate was acquired.</param>
    /// <param name="holder">The gate holder site.</param>
    internal void ReportMutationGateHold(long acquiredTimestamp, string holder)
    {
        var heldMs = ElapsedMs(acquiredTimestamp);
        if (heldMs < WarningThresholdMs)
            return;

        // Diagnostics only: a faulty log sink must not mask the gate holder's own outcome.
        TraceLogFailure(
            Isolated.Run((Logger: _logger, HeldMs: heldMs, Holder: holder), static state => ServerLog.JournalMutationGateHeldLong(state.Logger, state.HeldMs, state.Holder)));
    }

    /// <summary>
    /// Warns that a wait on the journal was canceled while the journal was stalled (a segment I/O call or a mutation-gate
    /// hold at or beyond the threshold), so the holder is visible at the moment a commit budget expires; never throws.
    /// </summary>
    /// <param name="waitingFor">What the canceled wait was waiting for.</param>
    /// <param name="ioOperation">The segment I/O operation in progress on the journal thread, if any.</param>
    /// <param name="ioMs">How long that operation has been running, or zero when none is.</param>
    /// <param name="gateHolder">The current mutation-gate holder site, if the gate is held.</param>
    /// <param name="gateHeldMs">How long the gate has been held, or zero when it is free.</param>
    internal void ReportWaitCanceled(string waitingFor, string? ioOperation, long ioMs, string? gateHolder, long gateHeldMs)
    {
        if (ioMs < WarningThresholdMs && gateHeldMs < WarningThresholdMs)
            return;

        // Diagnostics only: a faulty log sink must not mask the cancellation of the wait.
        TraceLogFailure(
            Isolated.Run(
                (Logger: _logger, WaitingFor: waitingFor, IoOperation: ioOperation ?? "none", IoMs: ioMs, GateHolder: gateHolder ?? "none", GateHeldMs: gateHeldMs),
                static state => ServerLog.JournalWaitCanceledWhileStalled(state.Logger, state.WaitingFor, state.IoOperation, state.IoMs, state.GateHolder, state.GateHeldMs)));
    }

    private static void TraceLogFailure(Exception? failure)
    {
        if (failure != null)
            Trace.TraceError($"Journal slow-operation warning could not be logged: {failure}");
    }

    private long ElapsedMs(long startedTimestamp) => Convert.ToInt64(_timeProvider.GetElapsedTime(startedTimestamp).TotalMilliseconds);
}
