using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Threading;

namespace Squirix.Server.Node.Services;

/// <summary>Decides when the next iteration of the first start of a replica group runs.</summary>
/// <remarks>
/// <para>
/// After an iteration the pacer waits for a fixed backoff, from <see cref="ReplicaGroupBootstrapService.InitialDelay" /> doubling up to
/// <see cref="ReplicaGroupBootstrapService.MaxDelay" />, or until the member the iteration waits for calls this node, whichever comes first;
/// a call of any other peer says nothing about that member and does not end the wait. A wake restarts the backoff.
/// </para>
/// <para>
/// A wake runs the next iteration at once, with one exception: when the iteration found the same outcome as the one before it, the member
/// that woke it did not change what it waits for, and the next iteration starts no sooner than <see cref="ReplicaGroupBootstrapService.InitialDelay" />
/// after the last one started. So two owners that wait for each other, whatever the reason, call each other at most once per initial delay,
/// while a member that comes up changes the outcome and is answered at once, with no time passing.
/// </para>
/// <para>Only the loop of the first start calls it.</para>
/// </remarks>
[Mutable]
internal sealed class ReplicaBootstrapPacer
{
    private readonly ReplicaPeerContact _contact;
    private readonly TimeProvider _timeProvider;

    private TimeSpan _backoff = ReplicaGroupBootstrapService.InitialDelay;
    private ReplicaBootstrapOutcome? _previous;
    private bool _repeated;
    private long _startedAt;

    /// <summary>Initializes a new instance of the <see cref="ReplicaBootstrapPacer" /> class.</summary>
    /// <param name="contact">The record of the peers that called this node.</param>
    /// <param name="timeProvider">Time source for the delays.</param>
    internal ReplicaBootstrapPacer(ReplicaPeerContact contact, TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(contact);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _contact = contact;
        _timeProvider = timeProvider;
    }

    /// <summary>Marks the start of an iteration, which reads the state of every peer itself.</summary>
    internal void Begin()
    {
        _contact.Reset();
        _startedAt = _timeProvider.GetTimestamp();
    }

    /// <summary>Waits until the next iteration is due.</summary>
    /// <param name="outcome">What the iteration that just ended did; <see langword="null" /> when it failed and is retried.</param>
    /// <param name="cancellationToken">Cancellation token; its cancellation ends the wait with <see cref="TimedWaitResult.Canceled" />.</param>
    /// <returns><see cref="TimedWaitResult.Canceled" /> when the token was canceled and no iteration is due; otherwise the next iteration is due.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The wait ended with a result that is not a named value.</exception>
    internal async Task<TimedWaitResult> WaitAsync(ReplicaBootstrapOutcome? outcome, CancellationToken cancellationToken)
    {
        _repeated = outcome == _previous;
        _previous = outcome;
        var awaited = outcome?.NodeId ?? string.Empty;
        var waitStarted = _timeProvider.GetTimestamp();
        while (true)
        {
            var remaining = _backoff - _timeProvider.GetElapsedTime(waitStarted);
            var contact = remaining <= TimeSpan.Zero
                ? TimedWaitResult.TimedOut
                : await _contact.WaitAsync(remaining, _timeProvider, cancellationToken).ConfigureAwait(false);
            switch (contact)
            {
                case TimedWaitResult.Canceled:
                    return TimedWaitResult.Canceled;
                case TimedWaitResult.TimedOut:
                    _backoff = TimeSpan.FromTicks(Math.Min(ReplicaGroupBootstrapService.MaxDelay.Ticks, _backoff.Ticks * 2));
                    return TimedWaitResult.TimedOut;
                case TimedWaitResult.Signaled:
                    break;
                default:
                    throw new ArgumentOutOfRangeException(nameof(outcome), contact, "Unsupported timed wait result.");
            }

            if (awaited.Length == 0 || !_contact.Contacted(awaited))
                continue;

            _backoff = ReplicaGroupBootstrapService.InitialDelay;
            return await WaitForFloorAsync(cancellationToken).ConfigureAwait(false);
        }
    }

    private async Task<TimedWaitResult> WaitForFloorAsync(CancellationToken cancellationToken)
    {
        var remaining = _repeated ? ReplicaGroupBootstrapService.InitialDelay - _timeProvider.GetElapsedTime(_startedAt) : TimeSpan.Zero;
        if (remaining <= TimeSpan.Zero)
            return TimedWaitResult.Signaled;

        await Task.Delay(remaining, _timeProvider, cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        return cancellationToken.IsCancellationRequested ? TimedWaitResult.Canceled : TimedWaitResult.Signaled;
    }
}
