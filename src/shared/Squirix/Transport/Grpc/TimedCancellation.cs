using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Squirix.Transport.Grpc;

/// <summary>A cancellation source that cancels when a timeout elapses on a time provider or when a parent token cancels.</summary>
/// <remarks>
/// A linked source over a separate timer source costs two sources and two registrations. This source carries the timer itself and forwards
/// the parent token with one registration, or none when the parent cannot be canceled. Dispose it exactly once, with
/// <see langword="using" />, after the guarded work completed: it unregisters from the parent, waiting for a running forwarding callback
/// so none can touch the disposed source, and stops the timer.
/// </remarks>
internal sealed class TimedCancellation : CancellationTokenSource
{
    private readonly CancellationTokenRegistration _parentRegistration;

    /// <summary>Initializes a new instance of the <see cref="TimedCancellation" /> class that cancels after <paramref name="timeout" /> or when <paramref name="parent" /> cancels.</summary>
    /// <param name="timeout">The time after which the source cancels; <see cref="TimeSpan.Zero" /> cancels at once.</param>
    /// <param name="timeProvider">The clock the timeout runs on.</param>
    /// <param name="parent">The token to forward; a token that is already canceled cancels the source before the constructor returns.</param>
    internal TimedCancellation(TimeSpan timeout, TimeProvider timeProvider, CancellationToken parent)
        : base(timeout, timeProvider)
    {
        if (!parent.CanBeCanceled)
            return;

        try
        {
            _parentRegistration = parent.UnsafeRegister(ForwardCancellation, this);
        }
        catch
        {
            base.Dispose(true);
            throw;
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
            _parentRegistration.Dispose();

        base.Dispose(disposing);
    }

    // The forwarded cancellation must run its callbacks on the canceling thread, as a linked source does; CancelAsync would not.
#pragma warning disable MA0045
    private static void ForwardCancellation(object? state) => Unsafe.As<TimedCancellation>(state)!.Cancel();
#pragma warning restore MA0045
}
