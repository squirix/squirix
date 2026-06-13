using System;
using JetBrains.Annotations;

namespace Squirix.Server.Errors;

/// <summary>
/// Thrown when a cache mutation is rejected under critical memory pressure (admission control).
/// Transports map this failure to appropriate REST/gRPC status codes via <see cref="PublicErrorCode" /> and <see cref="StableDetail" />.
/// This is a deterministic capacity signal; it does not indicate recovery from <see cref="OutOfMemoryException" />.
/// </summary>
public sealed class ResourceExhaustedException : Exception
{
    /// <summary>
    /// Stable machine-readable error code used in REST <c>code</c> and docs (<c>MEMORY_PRESSURE</c>).
    /// </summary>
    public const string PublicErrorCode = "MEMORY_PRESSURE";

    /// <summary>
    /// Stable, bounded detail text shared with REST/gRPC mappings (no raw keys, values, or cache names).
    /// </summary>
    public const string StableDetail = "The cache rejected this operation because estimated cache memory usage is critical.";

    /// <summary>
    /// Initializes a new instance of the <see cref="ResourceExhaustedException" /> class with the stable detail message.
    /// </summary>
    public ResourceExhaustedException()
        : base(StableDetail)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ResourceExhaustedException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    [PublicAPI]
    public ResourceExhaustedException(string message)
        : base(message)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ResourceExhaustedException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public ResourceExhaustedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }
}
