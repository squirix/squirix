using System;
using JetBrains.Annotations;

namespace Squirix.Server.Errors;

/// <summary>Thrown when an append or segment roll would exceed configured on-disk journal capacity.</summary>
[PublicAPI]
public sealed class JournalCapacityExceededException : Exception
{
    /// <summary>Stable, bounded detail text shared with gRPC and health/metrics HTTP error mappings (no raw paths, keys, or sizes).</summary>
    internal const string StableDetail = "The cache rejected this operation because on-disk journal usage is at the configured limit.";

    /// <summary>
    /// Initializes a new instance of the <see cref="JournalCapacityExceededException" /> class.
    /// </summary>
    public JournalCapacityExceededException()
        : base(StableDetail)
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
