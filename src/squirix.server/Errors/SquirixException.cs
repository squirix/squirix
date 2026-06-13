using System;
using JetBrains.Annotations;

namespace Squirix.Server.Errors;

/// <summary>
/// Represents a bounded squirix error with a stable machine-readable code.
/// </summary>
public sealed class SquirixException : Exception
{
    /// <summary>
    /// Initializes a new instance of the <see cref="SquirixException" /> class.
    /// </summary>
    [PublicAPI]
    public SquirixException()
    {
        Error = string.Empty;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SquirixException" /> class with a message.
    /// </summary>
    /// <param name="message">The exception message.</param>
    public SquirixException(string message)
        : base(message)
    {
        Error = message;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SquirixException" /> class with a message and inner exception.
    /// </summary>
    /// <param name="message">The exception message.</param>
    /// <param name="innerException">The inner exception.</param>
    public SquirixException(string message, Exception innerException)
        : base(message, innerException)
    {
        Error = message;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SquirixException" /> class.
    /// </summary>
    /// <param name="code">Stable squirix error code.</param>
    /// <param name="error">Stable error name.</param>
    /// <param name="detail">Optional bounded detail text.</param>
    public SquirixException(SquirixErrorCode code, string error, string? detail = null)
        : base(detail ?? error)
    {
        Code = code;
        Error = error;
        Detail = detail;
    }

    /// <summary>
    /// Gets the stable squirix error code.
    /// </summary>
    public SquirixErrorCode Code { get; }

    /// <summary>
    /// Gets optional bounded detail text.
    /// </summary>
    public string? Detail { get; }

    /// <summary>
    /// Gets the stable error name.
    /// </summary>
    public string Error { get; }
}
