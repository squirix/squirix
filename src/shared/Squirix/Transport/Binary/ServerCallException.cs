using System;
using System.Diagnostics.CodeAnalysis;
using Squirix.Transport.Wire.Cache;

namespace Squirix.Transport.Binary;

/// <summary>A failed server operation with the status and application metadata visible to its caller.</summary>
[SuppressMessage("Design", "CA1064:Exceptions should be public", Justification = "The peer failure is translated at the transport boundary and is not an exported exception.")]
[SuppressMessage("Major Code Smell", "S3871:Exception types should be public", Justification = "The peer failure is internal to the server transport.")]
internal sealed class ServerCallException : Exception
{
    public ServerCallException()
        : this(string.Empty)
    {
    }

    public ServerCallException(string message)
        : this(FrameStatus.Unknown, message)
    {
    }

    public ServerCallException(string message, Exception innerException)
        : this(FrameStatus.Unknown, message, innerException)
    {
    }

    internal ServerCallException(FrameStatus statusCode, string detail, Exception? cause = null)
        : this(statusCode, new BinaryCallError { Detail = detail }, cause)
    {
    }

    internal ServerCallException(FrameStatus statusCode, BinaryCallError error, Exception? cause = null)
        : base(error.Detail, cause)
    {
        StatusCode = statusCode;
        Error = error;
    }

    internal FrameStatus StatusCode { get; }

    internal BinaryCallError Error { get; }

    internal string? FindTrailer(string key)
    {
        foreach (var trailer in Error.Trailers)
        {
            if (trailer.ValueCase == BinaryCallError.Types.Trailer.ValueOneofCase.Text && string.Equals(trailer.Key, key, StringComparison.OrdinalIgnoreCase))
                return trailer.Text;
        }

        return null;
    }
}
