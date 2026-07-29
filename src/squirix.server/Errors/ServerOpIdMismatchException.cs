using System;
using JetBrains.Annotations;

namespace Squirix.Server.Errors;

/// <summary>
/// Thrown when a mutating RPC reuses an <c>operation_id</c> with a different mutation fingerprint.
/// </summary>
public sealed class ServerOpIdMismatchException : Exception
{
    /// <summary>Stable, bounded detail text shared with gRPC and health/metrics HTTP error mappings.</summary>
    internal const string StableDetail = "operation_id was reused with a different mutation fingerprint.";

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerOpIdMismatchException" /> class.
    /// </summary>
    public ServerOpIdMismatchException()
        : base(StableDetail)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerOpIdMismatchException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    [PublicAPI]
    public ServerOpIdMismatchException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ServerOpIdMismatchException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ServerOpIdMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
