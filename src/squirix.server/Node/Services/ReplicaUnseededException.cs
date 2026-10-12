using System;

namespace Squirix.Server.Node.Services;

/// <summary>Thrown when a group is to be led over a copy that is not complete: the leadership refuses its start, and nothing was written.</summary>
internal sealed class ReplicaUnseededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaUnseededException" /> class.</summary>
    internal ReplicaUnseededException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaUnseededException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal ReplicaUnseededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaUnseededException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal ReplicaUnseededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
