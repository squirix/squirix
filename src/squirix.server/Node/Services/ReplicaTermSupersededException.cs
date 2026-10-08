using System;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Thrown when the durable term of a group log moved past the led term, at the start of a leadership or at a local append the log refused:
/// a newer leader exists, and nothing was written.
/// </summary>
internal sealed class ReplicaTermSupersededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="ReplicaTermSupersededException" /> class.</summary>
    internal ReplicaTermSupersededException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaTermSupersededException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal ReplicaTermSupersededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="ReplicaTermSupersededException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal ReplicaTermSupersededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
