using System;
using JetBrains.Annotations;

namespace Squirix.Server.Storage.Journaling;

/// <summary>Thrown when pipelined segment roll or compaction cannot free enough capacity.</summary>
[PublicAPI]
public sealed class JournalCapacityExceededException : InvalidOperationException
{
    /// <summary>
    /// Initializes a new instance of the <see cref="JournalCapacityExceededException" /> class.
    /// </summary>
    public JournalCapacityExceededException()
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="JournalCapacityExceededException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public JournalCapacityExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="JournalCapacityExceededException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public JournalCapacityExceededException(string message)
        : base(message)
    {
    }
}
