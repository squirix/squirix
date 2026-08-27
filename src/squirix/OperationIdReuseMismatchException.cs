using System;
using JetBrains.Annotations;

namespace Squirix;

/// <summary>
/// Thrown when a mutating cache RPC reuses an <c language="csharp">operation_id</c> with a different mutation fingerprint.
/// </summary>
[PublicAPI]
public sealed class OperationIdReuseMismatchException : Exception
{
    /// <summary>Stable detail shared with the server gRPC contract.</summary>
    internal const string StableDetail = "operation_id was reused with a different mutation fingerprint.";

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
