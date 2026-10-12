using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Node.Services;

/// <summary>Keeps the log of a led group from being compacted while a state transfer to one of its followers is alive.</summary>
/// <remarks>
/// The transfer sends the state at an applied index and the follower then reads everything above it from the log, so the entries above
/// that index and the term at it must stay. The pin lives in memory only: a leader that crashes loses it, and so loses the transfer.
/// Each transfer takes its own <see cref="Session" />, which releases its hold exactly once on every exit.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaTransferPin
{
    private int _holds;

    /// <summary>Gets a value indicating whether a transfer session holds the pin.</summary>
    internal bool IsHeld => Volatile.Read(ref _holds) > 0;

    /// <summary>Creates the session of one transfer attempt; it holds nothing until <see cref="Session.Hold" /> runs.</summary>
    /// <returns>The session, which the caller disposes when the attempt ends, whatever way.</returns>
    internal Session Begin() => new(this);

    /// <summary>The hold of one transfer attempt on a <see cref="ReplicaTransferPin" />.</summary>
    [ThreadSafe]
    internal sealed class Session : IDisposable
    {
        private readonly ReplicaTransferPin _owner;
        private int _held;

        internal Session(ReplicaTransferPin owner)
        {
            _owner = owner;
        }

        /// <summary>Gets the applied index the pin was taken at; zero before it is taken.</summary>
        internal ulong Index { get; private set; }

        /// <summary>Releases the hold if it was taken; idempotent.</summary>
        public void Dispose()
        {
            if (Interlocked.Exchange(ref _held, 0) != 0)
                _ = Interlocked.Decrement(ref _owner._holds);
        }

        /// <summary>Takes the hold at an applied index; a second call keeps the first.</summary>
        /// <param name="index">The applied index the transferred state is taken at.</param>
        /// <remarks>Called under the commit gate of the committer, before it is left, so compaction under the same gate sees the hold.</remarks>
        internal void Hold(ulong index)
        {
            if (Interlocked.Exchange(ref _held, 1) != 0)
                return;

            Index = index;
            _ = Interlocked.Increment(ref _owner._holds);
        }
    }
}
