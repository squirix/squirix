using System;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Replication;

/// <summary>
/// A replicated write was refused because of shutdown before any entry of the write was scheduled for disk. Nothing of the write can be durable
/// through the refused step, so the caller may answer that the write was not accepted. A failure at or after the scheduling of the entries is
/// never reported with this type.
/// </summary>
[UsedImplicitly]
#pragma warning disable RCS1194 // ObjectDisposedException has no parameterless constructor to forward to, so the standard set cannot be matched exactly.
internal sealed class ReplicaShutdownRefusedException : ObjectDisposedException
#pragma warning restore RCS1194
{
    private const string ExceptionMessage = "The replicated write was refused by shutdown before it was scheduled.";

    /// <summary>Initializes a new instance of the <see cref="ReplicaShutdownRefusedException" /> class.</summary>
    internal ReplicaShutdownRefusedException()
        : base(ExceptionMessage)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaShutdownRefusedException" /> class with the refusing component.</summary>
    /// <param name="message">The name of the component that refused the write, reported as the object name.</param>
    internal ReplicaShutdownRefusedException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaShutdownRefusedException" /> class with the exception that caused the refusal.</summary>
    /// <param name="innerException">The exception that caused the refusal.</param>
    internal ReplicaShutdownRefusedException(Exception innerException)
        : base(ExceptionMessage, innerException)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaShutdownRefusedException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The exception that caused the refusal.</param>
    internal ReplicaShutdownRefusedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
