using System;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>The install gate of one group: the one transfer ticket it may hold, the gate that serializes the messages, and the idle timer.</summary>
/// <remarks>
/// The messages of a group hold <see cref="Gate" /> while they are handled. The idle timer cannot, so every change of the ticket slot, and the
/// end callback that releases the group, run under one lock; the timer drops a ticket only while no message is handled and the ticket has been
/// silent for the idle timeout. The callback does no I/O.
/// </remarks>
[ThreadSafe]
internal sealed class ReplicaTransferSlot : IDisposable
{
    private readonly TimeProvider _clock;
    private readonly Action<ReplicaTransferTicket, ReplicaTransferEnd> _ended;
    private readonly TimeSpan _idle;
    private readonly Lock _sync = new();
    private readonly ITimer _timer;
    private ReplicaTransferTicket? _ticket;

    /// <summary>Initializes a new instance of the <see cref="ReplicaTransferSlot" /> class.</summary>
    /// <param name="clock">Time source of the activity stamps and the idle timer.</param>
    /// <param name="idle">The longest silence a ticket survives.</param>
    /// <param name="ended">Releases the group when a ticket is dropped; called under the slot lock and must not block.</param>
    internal ReplicaTransferSlot(TimeProvider clock, TimeSpan idle, Action<ReplicaTransferTicket, ReplicaTransferEnd> ended)
    {
        ArgumentNullException.ThrowIfNull(clock);
        ArgumentNullException.ThrowIfNull(ended);
        _clock = clock;
        _idle = idle;
        _ended = ended;
        _timer = clock.CreateTimer(
            static state =>
            {
                if (state is ReplicaTransferSlot slot)
                    slot.OnIdleTimer();
            },
            this,
            Timeout.InfiniteTimeSpan,
            Timeout.InfiniteTimeSpan);
    }

    /// <summary>Gets the ticket the group holds, if any.</summary>
    internal ReplicaTransferTicket? Current
    {
        get
        {
            lock (_sync)
                return _ticket;
        }
    }

    /// <summary>Gets the gate every message of the group holds while it is handled.</summary>
    internal SemaphoreSlim Gate { get; } = new(1, 1);

    /// <inheritdoc />
    public void Dispose()
    {
        _timer.Dispose();
        Gate.Dispose();
    }

    /// <summary>Drops a ticket the slot still holds.</summary>
    /// <param name="ticket">The ticket to drop.</param>
    /// <param name="end">Why it is dropped.</param>
    /// <returns><see langword="true" /> when the ticket was the current one and the group is released; <see langword="false" /> when it was dropped before.</returns>
    internal bool Drop(ReplicaTransferTicket ticket, ReplicaTransferEnd end)
    {
        lock (_sync)
        {
            if (!ReferenceEquals(_ticket, ticket))
                return false;

            DropLocked(ticket, end);
            return true;
        }
    }

    /// <summary>Ends the handling of an admitted message that kept the ticket.</summary>
    /// <param name="ticket">The admitted ticket.</param>
    /// <param name="accepted">Whether the message was accepted, which advances the expected sequence.</param>
    internal void Finish(ReplicaTransferTicket ticket, bool accepted)
    {
        lock (_sync)
        {
            ticket.Busy = false;
            if (!ReferenceEquals(_ticket, ticket))
                return;

            ticket.LastActivity = _clock.GetTimestamp();
            if (accepted)
                ticket.Advance();
        }
    }

    /// <summary>Tells whether a ticket has been silent for the idle timeout.</summary>
    /// <param name="ticket">The ticket.</param>
    /// <returns><see langword="true" /> when no message was handled for the idle timeout.</returns>
    internal bool IsIdle(ReplicaTransferTicket ticket)
    {
        lock (_sync)
            return _clock.GetElapsedTime(ticket.LastActivity) >= _idle;
    }

    /// <summary>Makes a ticket the held one and starts its idle timer.</summary>
    /// <param name="ticket">The new ticket; the slot holds none.</param>
    internal void Open(ReplicaTransferTicket ticket)
    {
        lock (_sync)
        {
            _ticket = ticket;
            _ = _timer.Change(_idle, Timeout.InfiniteTimeSpan);
        }
    }

    /// <summary>Admits a message of the held transfer.</summary>
    /// <param name="header">The transfer the message names, with its verified sender and term.</param>
    /// <param name="sequence">The sequence number the message carries.</param>
    /// <returns>The ticket, now busy, or <see langword="null" /> when the group holds no transfer with this id, sender and term, or expects another sequence.</returns>
    internal ReplicaTransferTicket? Admit(in ReplicaTransferHeader header, int sequence)
    {
        lock (_sync)
        {
            if (_ticket is not { } ticket || ticket.Header != header || ticket.ExpectedSequence != sequence)
                return null;

            ticket.Busy = true;
            return ticket;
        }
    }

    private void DropLocked(ReplicaTransferTicket ticket, ReplicaTransferEnd end)
    {
        _ticket = null;
        ticket.Busy = false;
        ticket.ReleaseImage();
        _ = _timer.Change(Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);
        _ended(ticket, end);
    }

    /// <summary>Drops the ticket when it has been silent for the idle timeout and no message is handled; otherwise waits for the rest of the timeout.</summary>
    private void OnIdleTimer()
    {
        lock (_sync)
        {
            if (_ticket is not { } ticket)
                return;

            var silent = _clock.GetElapsedTime(ticket.LastActivity);
            if (!ticket.Busy && silent >= _idle)
                DropLocked(ticket, ReplicaTransferEnd.Expired);
            else
                _ = _timer.Change(ticket.Busy ? _idle : _idle - silent, Timeout.InfiniteTimeSpan);
        }
    }
}
