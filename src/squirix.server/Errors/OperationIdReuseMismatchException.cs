using System;
using JetBrains.Annotations;

namespace Squirix.Server.Errors;

/// <summary>
/// Thrown when a mutating RPC reuses an <c>operation_id</c> with a different mutation fingerprint.
/// </summary>
public sealed class OperationIdReuseMismatchException : Exception
{
    /// <summary>
    /// Stable, bounded detail text shared with REST/gRPC mappings.
    /// </summary>
    public const string StableDetail = "operation_id was reused with a different mutation fingerprint.";

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdReuseMismatchException" /> class.
    /// </summary>
    public OperationIdReuseMismatchException()
        : base(StableDetail)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdReuseMismatchException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    [PublicAPI]
    public OperationIdReuseMismatchException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdReuseMismatchException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public OperationIdReuseMismatchException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
