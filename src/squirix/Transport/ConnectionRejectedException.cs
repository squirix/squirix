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

    public ConnectionRejectedException(string message)
        : base(message)
    {
    }

    public ConnectionRejectedException(string message, Exception innerException)
        : base(message, innerException)
    {
    }

    public ConnectionRejectedException(string message, int hresult)
        : base(message, hresult)
    {
    }

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
