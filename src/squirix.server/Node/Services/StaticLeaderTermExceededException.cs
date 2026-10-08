using System;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Thrown when a group this node leads without an election has a log term above one, which only an election sets: the static leader
/// refuses its start, and nothing was written.
/// </summary>
internal sealed class StaticLeaderTermExceededException : InvalidOperationException
{
    /// <summary>Initializes a new instance of the <see cref="StaticLeaderTermExceededException" /> class.</summary>
    internal StaticLeaderTermExceededException()
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StaticLeaderTermExceededException" /> class with a message.</summary>
    /// <param name="message">The exception message.</param>
    internal StaticLeaderTermExceededException(string message)
        : base(message)
    {
    }

    /// <summary>Initializes a new instance of the <see cref="StaticLeaderTermExceededException" /> class with a message and inner exception.</summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    internal StaticLeaderTermExceededException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
