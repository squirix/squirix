using System;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Journaling.Abstractions;

/// <summary>
/// Thrown by a journal append whose frame already entered the ring when the wait for the journal thread's write ack faults (shutdown or the
/// pipeline failure latch): the frame may still become durable, so the append outcome is unknown, not failed.
/// </summary>
/// <remarks>
/// Faults before the ring enqueue keep their own exception, and so does a capacity rejection delivered through the write ack (the frame
/// is never written). The original fault is the <see cref="Exception.InnerException" />.
/// </remarks>
internal sealed class JournalPostEnqueueFaultException : InvalidOperationException
{
    /// <summary>Message of a faulted write ack wait.</summary>
    internal const string WriteAckFaultedMessage = "journal frame entered the ring but its write was not confirmed; it may still become durable.";

    /// <summary>Initializes a new instance of the <see cref="JournalPostEnqueueFaultException" /> class.</summary>
    [UsedImplicitly]
    public JournalPostEnqueueFaultException()
        : base(WriteAckFaultedMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JournalPostEnqueueFaultException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    [UsedImplicitly]
    public JournalPostEnqueueFaultException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="JournalPostEnqueueFaultException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The fault of the write ack wait.</param>
    public JournalPostEnqueueFaultException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
