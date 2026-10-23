using System;
using System.Runtime.CompilerServices;
using System.Threading;

namespace Squirix.Server.Threading;

/// <summary>Helpers that end a <see cref="PeriodicTimer" /> loop on cancellation without an exception.</summary>
internal static class PeriodicTimerExtensions
{
    /// <param name="timer">The timer a loop waits on.</param>
    extension(PeriodicTimer timer)
    {
        /// <summary>Disposes the timer when <paramref name="cancellationToken" /> is canceled, so a pending or later wait returns <see langword="false" /> instead of throwing.</summary>
        /// <param name="cancellationToken">The token whose cancellation ends the loop; the loop itself waits without it.</param>
        /// <returns>The registration; dispose it when the loop ends.</returns>
        /// <remarks><see cref="PeriodicTimer.Dispose" /> is safe to call concurrently with a wait and more than once.</remarks>
        internal CancellationTokenRegistration DisposeOnCancel(CancellationToken cancellationToken)
        {
            ArgumentNullException.ThrowIfNull(timer);
            return cancellationToken.UnsafeRegister(static state => Unsafe.As<PeriodicTimer>(state)!.Dispose(), timer);
        }
    }
}
