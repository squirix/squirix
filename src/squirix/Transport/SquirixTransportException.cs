using System;

namespace Squirix.Transport;

/// <summary>Reports a failure reported by the Squirix client transport.</summary>
public sealed class SquirixTransportException : Exception
{
    /// <summary>Initializes a new instance of the <see cref="SquirixTransportException" /> class with an unclassified status.</summary>
    public SquirixTransportException()
        : this("The cache transport failed.")
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SquirixTransportException" /> class with an unclassified status.</summary>
    /// <param name="message">The failure description.</param>
    public SquirixTransportException(string message)
        : this(new CallStatus(CacheTransportStatusCode.Unknown, message))
    {
    }

    /// <summary>Initializes a new instance of the <see cref="SquirixTransportException" /> class with an unclassified status and a cause.</summary>
    /// <param name="message">The failure description.</param>
    /// <param name="innerException">The underlying exception.</param>
    public SquirixTransportException(string message, Exception innerException)
        : this(new CallStatus(CacheTransportStatusCode.Unknown, message, innerException))
    {
    }

    internal SquirixTransportException(in CallStatus status)
        : base(status.Detail, status.Cause)
    {
        Status = status;
    }

    /// <summary>Gets the classified transport failure.</summary>
    public CacheTransportStatusCode StatusCode => Status.Code;

    internal CallStatus Status { get; }
}
