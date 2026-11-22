using System;
using System.IO;

namespace Squirix.Transport;

/// <summary>Identifies an explicit authentication rejection received over the verified TLS transport.</summary>
internal sealed class ConnectionRejectedException : IOException
{
    public ConnectionRejectedException()
        : base("The server rejected connection authentication.")
    {
    }

    internal ConnectionRejectedException(string message)
        : base(message)
    {
    }

    internal ConnectionRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    internal ConnectionRejectedException(string message, int hresult)
        : base(message, hresult)
    {
    }

    internal bool PrefaceRejected { get; init; }

    internal static SquirixTransportException? Map(Exception exception)
    {
        for (var current = exception; current != null; current = current.InnerException)
        {
            if (current is ConnectionRejectedException)
                return new SquirixTransportException(new CallStatus(CacheTransportStatusCode.Unauthenticated, "Connection authentication was rejected."));
        }

        return null;
    }
}
