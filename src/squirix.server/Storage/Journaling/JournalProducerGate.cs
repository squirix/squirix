using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Threading;

namespace Squirix.Server.Storage.Journaling;

/// <summary>
/// Quiesces journal producers before the shutdown marker: once shutdown is initiated, new work is
/// rejected, and disposal waits until in-flight enqueues are published to the ring, so the FIFO
/// shutdown marker can never overtake an admitted append.
/// </summary>
internal sealed class JournalProducerGate
{
    private readonly QuiescenceGate _drain = new();
    private int _shutdownInitiated;

    internal void Enter() => _drain.Enter();

    internal void Exit() => _drain.Exit();

    internal void InitiateShutdown() => Volatile.Write(ref _shutdownInitiated, 1);

    internal void ThrowIfShutdownInitiated() => ObjectDisposedException.ThrowIf(Volatile.Read(ref _shutdownInitiated) != 0, this);

    /// <summary>Waits until in-flight enqueues are published, giving up after the timeout.</summary>
    /// <param name="timeout">Maximum time to wait for producers to drain.</param>
    /// <returns>Whether all in-flight enqueues were published in time.</returns>
    internal async ValueTask<bool> WaitAsync(TimeSpan timeout)
    {
        // Hitting the timeout means the journal thread itself is wedged (producer sections cover
        // only the shutdown check plus ring publish); the caller must then proceed best-effort and
        // surface the failure loudly instead of hanging disposal forever.
        using var cts = new CancellationTokenSource(timeout);
        try
        {
            await _drain.WaitAsync(cts.Token).ConfigureAwait(false);
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }
}
