using System.Diagnostics;
using System.Threading;
using Microsoft.Extensions.Logging;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Tracks the journal I/O in progress and the mutation gate holder, and reports a stalled journal when a wait on it is canceled.</summary>
internal sealed class JournalStallProbe
{
    private readonly StallSlot _gate = new();
    private readonly StallSlot _io = new();
    private readonly ILogger _log;

    internal JournalStallProbe(ILogger log)
    {
        _log = log;
    }

    /// <summary>Records that the mutation gate was acquired by <paramref name="holder" />.</summary>
    /// <param name="holder">The gate holder site.</param>
    /// <returns>The acquisition timestamp.</returns>
    internal long GateAcquired(string holder) => _gate.Begin(holder);

    /// <summary>Records that the mutation gate was released.</summary>
    internal void GateReleased() => _gate.End();

    /// <summary>Records that a journal segment I/O operation started.</summary>
    /// <param name="operation">The operation name.</param>
    internal void IoStarted(string operation) => _ = _io.Begin(operation);

    /// <summary>Records that the journal segment I/O operation finished.</summary>
    internal void IoFinished() => _io.End();

    /// <summary>Reads the journal segment I/O operation in progress, if any; lock-free and allocation-free, safe from any thread.</summary>
    /// <param name="operation">The operation name, when one is in progress.</param>
    /// <param name="startedTimestamp">The <see cref="Stopwatch.GetTimestamp" /> value taken when the operation started.</param>
    /// <returns><see langword="true" /> when an operation is in progress; <see langword="false" /> when none is or the read kept racing a writer.</returns>
    internal bool TryReadIo(out string? operation, out long startedTimestamp) => _io.TryRead(out operation, out startedTimestamp);

    /// <summary>Logs the stall state when a wait on the journal was canceled while journal I/O or the gate is stalled.</summary>
    /// <param name="waitingFor">What the canceled wait was waiting for.</param>
    internal void ReportWaitCanceled(string waitingFor)
    {
        var ioActive = _io.TryRead(out var ioOperation, out var ioStarted);
        var gateActive = _gate.TryRead(out var gateHolder, out var gateAcquired);
        JournalSlowOperationDiagnostics.ReportWaitCanceled(
            _log,
            waitingFor,
            ioActive ? ioOperation : null,
            ioActive ? JournalSlowOperationDiagnostics.ElapsedMsSince(ioStarted) : 0,
            gateActive ? gateHolder : null,
            gateActive ? JournalSlowOperationDiagnostics.ElapsedMsSince(gateAcquired) : 0);
    }

    /// <summary>
    /// One label and start timestamp published together with a sequence counter, so a reader never pairs a label with the
    /// timestamp of another operation. Allocation-free; writers are serialized by the code path that owns the slot.
    /// </summary>
    private sealed class StallSlot
    {
        private string? _label;
        private long _started;
        private long _version;

        internal long Begin(string label)
        {
            var timestamp = Stopwatch.GetTimestamp();
            _ = Interlocked.Increment(ref _version);
            _label = label;
            _started = timestamp;
            Volatile.Write(ref _version, _version + 1);
            return timestamp;
        }

        internal void End()
        {
            _ = Interlocked.Increment(ref _version);
            _started = 0;
            Volatile.Write(ref _version, _version + 1);
        }

        internal bool TryRead(out string? label, out long started)
        {
            for (var attempt = 0; attempt < 8; attempt++)
            {
                var before = Volatile.Read(ref _version);
                if ((before & 1) != 0)
                    continue;

                var currentLabel = _label;
                var currentStarted = _started;
                Interlocked.MemoryBarrier();
                if (Volatile.Read(ref _version) != before)
                    continue;

                label = currentLabel;
                started = currentStarted;
                return currentStarted != 0;
            }

            label = null;
            started = 0;
            return false;
        }
    }
}
