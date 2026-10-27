using System;
using System.Runtime.InteropServices;
using Squirix.Server.Attributes;

namespace Squirix.Server.Threading;

/// <summary>
/// Owner capability for an <see cref="AsyncLock" />-protected surface: names one acquisition of one lock, so a guarded
/// API can tell the flow that holds the lock from any other flow.
/// </summary>
/// <remarks>
/// <para>
/// This is the general mechanism for surfaces an <see cref="AsyncLock" /> protects. An ownership is obtained only from
/// the holder the lock returned (<see cref="AsyncLockHolder.Ownership" />); the flow holding the lock passes it on to
/// whatever it calls under the lock, and the guarded API checks it with <see cref="Holds" /> or
/// <see cref="ThrowIfNotHeld" /> instead of asking whether the lock is held at all.
/// </para>
/// <para>
/// Every acquisition, a free take or a FIFO hand-off, issues a new generation, so an ownership holds only from its
/// acquisition until the holder releases: one kept past its release, one presented by a flow that does not hold the
/// lock while another flow does, one issued by a different lock, and a <see langword="default" /> ownership never hold.
/// Checking an ownership is lock-free and does not allocate.
/// </para>
/// </remarks>
[Immutable]
[StructLayout(LayoutKind.Auto)]
internal readonly record struct AsyncLockOwnership
{
    private readonly ulong _generation;
    private readonly AsyncLock? _lock;

    /// <summary>Initializes a new instance of the <see cref="AsyncLockOwnership" /> struct; only <see cref="AsyncLockHolder.Ownership" /> creates one.</summary>
    /// <param name="asyncLock">The lock that issued the acquisition.</param>
    /// <param name="generation">The ownership generation the lock issued for the acquisition.</param>
    internal AsyncLockOwnership(AsyncLock asyncLock, ulong generation)
    {
        _lock = asyncLock;
        _generation = generation;
    }

    /// <summary>Gets a value indicating whether this ownership currently holds <paramref name="asyncLock" />.</summary>
    /// <param name="asyncLock">The lock protecting the guarded surface.</param>
    /// <returns>
    /// <see langword="true" /> only when the acquisition was issued by <paramref name="asyncLock" /> and its holder has not
    /// released it.
    /// </returns>
    internal bool Holds(AsyncLock asyncLock) => _lock != null && ReferenceEquals(_lock, asyncLock) && asyncLock.IsHeldBy(_generation);

    /// <summary>Refuses the caller unless this ownership currently holds <paramref name="asyncLock" />.</summary>
    /// <param name="asyncLock">The lock protecting the guarded surface.</param>
    /// <param name="message">The refusal message naming the surface and how to hold its lock.</param>
    /// <exception cref="InvalidOperationException">The ownership does not hold <paramref name="asyncLock" />; a programming error in the caller.</exception>
    internal void ThrowIfNotHeld(AsyncLock asyncLock, string message)
    {
        if (!Holds(asyncLock))
            throw new InvalidOperationException(message);
    }
}
