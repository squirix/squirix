using System;
using JetBrains.Annotations;

namespace Squirix;

/// <summary>
/// Thrown when a mutating cache RPC is missing a required <c>operation_id</c>.
/// </summary>
[PublicAPI]
public sealed class OperationIdRequiredException : Exception
{
    /// <summary>Stable detail shared with the server gRPC contract.</summary>
    internal const string StableDetail = "operation_id is required for mutating cache RPCs.";

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdRequiredException" /> class.
    /// </summary>
    public OperationIdRequiredException()
        : base(StableDetail)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdRequiredException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public OperationIdRequiredException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="OperationIdRequiredException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public OperationIdRequiredException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
