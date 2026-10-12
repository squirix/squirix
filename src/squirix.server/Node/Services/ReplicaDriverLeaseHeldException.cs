using System;

namespace Squirix.Server.Node.Services;

/// <summary>Thrown when a leadership asks for the applier lease while another leadership or a received state transfer holds it; nothing changed.</summary>
internal sealed class ReplicaDriverLeaseHeldException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaDriverLeaseHeldException" /> class.</summary>
    internal ReplicaDriverLeaseHeldException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaDriverLeaseHeldException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal ReplicaDriverLeaseHeldException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaDriverLeaseHeldException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal ReplicaDriverLeaseHeldException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
