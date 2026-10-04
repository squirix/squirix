using System;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.Node.App;
using Squirix.Server.Storage.Journaling.Abstractions;

namespace Squirix.Server.Benchmarks;

/// <summary>
/// The writers and counters of one stall measurement cell: K closed-loop writers issue durable mutations through the production executor with a
/// client deadline; the server side gets the same deadline as its cancellation token, the client gives up a few milliseconds later.
/// </summary>
[ThreadSafe]
internal sealed class StallRun
{
    private static readonly TimeSpan ClientGrace = TimeSpan.FromMilliseconds(25);
    private readonly StallCellSpec _cell;
    private readonly JournalMeasurementHost _host;
    private readonly long _origin = Stopwatch.GetTimestamp();
    private readonly byte[] _payload;
    private int _appended;
    private int _held;
    private int _orphans;
    private int _serverInflight;
    private int _stop;
    private int _waiting;

    /// <summary>Initializes a new instance of the <see cref="StallRun" /> class.</summary>
    /// <param name="host">The journal host.</param>
    /// <param name="cell">The cell being measured.</param>
    internal StallRun(JournalMeasurementHost host, StallCellSpec cell)
    {
        _host = host;
        _cell = cell;
        _payload = new byte[cell.PayloadBytes];
        Array.Fill(_payload, Convert.ToByte('m'));
    }

    /// <summary>Gets the requests appended to the ring that wait for durability.</summary>
    internal int Appended => Volatile.Read(ref _appended);

    /// <summary>Gets the requests holding the mutation gate.</summary>
    internal int Held => Volatile.Read(ref _held);

    /// <summary>Gets how long each mutation held the gate, in milliseconds.</summary>
    internal ConcurrentQueue<double> Holds { get; } = new();

    /// <summary>Gets when each mutation committed on the server, in milliseconds since the start.</summary>
    internal ConcurrentQueue<double> OkCompletionMs { get; } = new();

    /// <summary>Gets the descriptions of failures other than cancellations.</summary>
    internal ConcurrentQueue<string> OtherErrors { get; } = new();

    /// <summary>Gets the requests the client gave up on that the server still runs.</summary>
    internal int Orphans => Volatile.Read(ref _orphans);

    /// <summary>Gets every finished request.</summary>
    internal ConcurrentQueue<StallRequest> Requests { get; } = new();

    /// <summary>Gets the requests the server still runs.</summary>
    internal int ServerInflight => Volatile.Read(ref _serverInflight);

    /// <summary>Gets the requests waiting for the mutation gate.</summary>
    internal int Waiting => Volatile.Read(ref _waiting);

    /// <summary>Gets the milliseconds since the run started.</summary>
    /// <returns>The elapsed milliseconds.</returns>
    internal double NowMs() => Stopwatch.GetElapsedTime(_origin).TotalMilliseconds;

    /// <summary>Asks the writers to stop after their current request.</summary>
    internal void RequestStop() => Volatile.Write(ref _stop, 1);

    /// <summary>Waits until the server runs no request any more, or the timeout passes.</summary>
    /// <param name="timeout">The longest wait.</param>
    /// <returns>An asynchronous operation.</returns>
    internal async Task WaitForServerIdleAsync(TimeSpan timeout)
    {
        var started = TimeProvider.System.GetTimestamp();
        while (ServerInflight > 0 && TimeProvider.System.GetElapsedTime(started) < timeout)
            await Task.Delay(TimeSpan.FromMilliseconds(50), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>Issues durable mutations back to back until <see cref="RequestStop" />; a request that the client gave up on keeps running on the server.</summary>
    /// <param name="writerId">The writer number, part of the keys.</param>
    /// <returns>An asynchronous operation.</returns>
    internal async Task WriterLoopAsync(int writerId)
    {
        await Task.Yield();
        var sequence = 0L;
        var deadline = TimeSpan.FromSeconds(_cell.DeadlineSeconds);
        while (Volatile.Read(ref _stop) == 0)
        {
            var request = new StallRequest { IssueMs = NowMs() };
            var op = new StallOp { Request = request };
            var key = new CacheKey("stall", string.Create(CultureInfo.InvariantCulture, $"w{writerId}-{sequence++}"));
            _ = Interlocked.Increment(ref _waiting);
            _ = Interlocked.Increment(ref _serverInflight);
            var execution = ExecuteAsync(op, key, deadline);
            var completed = await CompletesWithinAsync(execution, deadline + ClientGrace).ConfigureAwait(false);
            request.Outcome = Settle(op, completed);
            Requests.Enqueue(request);
            if (request.Outcome == StallOutcome.Error)
                await Task.Delay(TimeSpan.FromMilliseconds(10), TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
        }
    }

    private static async Task<bool> CompletesWithinAsync(Task task, TimeSpan window)
    {
        try
        {
            await task.WaitAsync(window, TimeProvider.System, CancellationToken.None).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }

    private static int Classify(StallOp op, bool gaveUp)
    {
        if (gaveUp)
            return op.Appended ? StallOutcome.TimedOutFrameQueued : StallOutcome.TimedOutNotAppended;

        if (op.Succeeded)
            return StallOutcome.Ok;

        var canceledOutcome = op.EnteredGate ? StallOutcome.CanceledInAppend : StallOutcome.CanceledAtGate;
        return op.Failure is OperationCanceledException ? canceledOutcome : StallOutcome.Error;
    }

    private int Settle(StallOp op, bool completed)
    {
        var gaveUp = false;
        lock (op)
        {
            op.Request.ClientEndMs = NowMs();
            if (!completed && !op.Done)
            {
                op.GaveUp = true;
                gaveUp = true;
                _ = Interlocked.Increment(ref _orphans);
            }
        }

        return Classify(op, gaveUp);
    }

    private async Task ExecuteAsync(StallOp op, CacheKey key, TimeSpan serverDeadline)
    {
        using var cts = new CancellationTokenSource(serverDeadline);
        try
        {
            _ = await _host.Executor.ExecuteAsync(
                key,
                static (s, _) =>
                {
                    s.Run.GateEntered(s.Op);
                    return ValueTask.FromResult(DurableMutationCondition<int>.Apply());
                },
                new DurableMutationPipeline<(StallRun Run, StallOp Op, IJournalCoordinator Journal, CacheKey Key, byte[] Payload, bool GroupCommit), int>(
                    (this, op, _host.Journal, key, _payload, _cell.GroupCommit),
                    static async (s, ownership, ct) =>
                    {
                        await s.Journal.AppendPutAsync(ownership, s.Key, s.Payload, ct).ConfigureAwait(false);
                        s.Run.AppendReturned(s.Op, s.GroupCommit);
                    },
                    static (s, _) =>
                    {
                        s.Run.MemoryApplied(s.Op, s.GroupCommit);
                        return new ValueTask<int>(1);
                    }),
                cts.Token).ConfigureAwait(false);
            op.Succeeded = true;
        }
        catch (Exception ex) when (ex is OperationCanceledException or InvalidOperationException or IOException or SquirixException)
        {
            op.Failure = ex;
            if (ex is not OperationCanceledException)
                OtherErrors.Enqueue(ex.GetType().Name + ": " + ex.Message);
        }
        finally
        {
            Finish(op);
        }
    }

    private void GateEntered(StallOp op)
    {
        op.EnteredGate = true;
        op.GateEnterMs = NowMs();
        _ = Interlocked.Decrement(ref _waiting);
        _ = Interlocked.Increment(ref _held);
    }

    private void AppendReturned(StallOp op, bool groupCommit)
    {
        op.Appended = true;
        _ = Interlocked.Increment(ref _appended);
        if (groupCommit)
            EndHold(op);
    }

    private void MemoryApplied(StallOp op, bool groupCommit)
    {
        if (!groupCommit)
            EndHold(op);
    }

    private void EndHold(StallOp op)
    {
        lock (op)
        {
            if (!op.EnteredGate || op.HoldEnded)
                return;

            op.HoldEnded = true;
        }

        Holds.Enqueue(NowMs() - op.GateEnterMs);
        _ = Interlocked.Decrement(ref _held);
    }

    private void Finish(StallOp op)
    {
        if (!op.EnteredGate)
            _ = Interlocked.Decrement(ref _waiting);

        EndHold(op);
        if (op.Appended)
            _ = Interlocked.Decrement(ref _appended);

        var now = NowMs();
        op.Request.ServerEndMs = now;
        op.Request.CommittedLater = op.Succeeded;
        if (op.Succeeded)
            OkCompletionMs.Enqueue(now);

        lock (op)
        {
            op.Done = true;
            if (op.GaveUp)
                _ = Interlocked.Decrement(ref _orphans);
        }

        _ = Interlocked.Decrement(ref _serverInflight);
    }
}
