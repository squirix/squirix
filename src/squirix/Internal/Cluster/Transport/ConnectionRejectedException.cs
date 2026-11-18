using System;
using System.IO;
using Grpc.Core;

namespace Squirix.Internal.Cluster.Transport;

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

    internal static RpcException? Map(Exception exception)
    {
        for (Exception? current = exception; current != null; current = current is RpcException rpc ? rpc.Status.DebugException ?? rpc.InnerException : current.InnerException)
        {
            if (current is ConnectionRejectedException)
                return new RpcException(new Status(StatusCode.Unauthenticated, "Connection authentication was rejected."));
        }

        return null;
    }
}
