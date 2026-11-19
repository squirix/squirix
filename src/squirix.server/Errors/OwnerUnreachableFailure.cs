using System;
using System.Net.Http;
using Squirix.Transport.Binary;

namespace Squirix.Server.Errors;

/// <summary>Builds and recognizes the failure of a forward from this node whose connection to the target node could not be established.</summary>
/// <remarks>
/// <para>
/// On the wire it is <see cref="FrameStatus.Unavailable" /> with <see cref="ServerOpContract.OwnerUnreachableDetail" />. On this node it also
/// carries the connection failure as its cause, which no status received from a peer carries, so a failure a peer relays with the same detail
/// is never taken for one. The forwarder raises it only when no attempt of the forward connected to the target; the request can then be routed elsewhere safely.
/// </para>
/// <para>
/// Only a failure the transport reports as a connect failure counts. A target whose host does not answer at all, or a connect slower than the
/// per-attempt timeout of the forward (a refused loopback connect takes about two seconds on Windows), ends as a timeout, which is ambiguous and
/// never reported as unreachable.
/// </para>
/// </remarks>
internal static class OwnerUnreachableFailure
{
    /// <summary>Creates the failure of a forward that could not connect to its target.</summary>
    /// <param name="cause">The connection failure; <see cref="IsConnectFailure" /> must accept it.</param>
    /// <returns><see cref="FrameStatus.Unavailable" /> with <see cref="ServerOpContract.OwnerUnreachableDetail" />: no attempt of the forward connected to its target.</returns>
    internal static ServerCallException Create(Exception cause) => new(FrameStatus.Unavailable, ServerOpContract.OwnerUnreachableDetail, cause);

    /// <summary>Determines whether a failure proves that the connection to the target was never established, so no byte of the request was sent.</summary>
    /// <param name="failure">The failure, or the cause a client-side status carries.</param>
    /// <returns>
    /// <see langword="true" /> when the first <see cref="HttpRequestException" /> in the chain failed to connect, to resolve the name, or to
    /// complete the TLS handshake; <see langword="false" /> for every failure that may follow a sent request.
    /// </returns>
    internal static bool IsConnectFailure(Exception? failure)
    {
        for (var current = failure; current != null; current = current.InnerException)
        {
            if (current is HttpRequestException http)
                return http.HttpRequestError is HttpRequestError.ConnectionError or HttpRequestError.NameResolutionError or HttpRequestError.SecureConnectionError;
        }

        return false;
    }

    /// <summary>Determines whether a failure is the one <see cref="Create" /> built on this node.</summary>
    /// <param name="exception">The failure of one attempt.</param>
    /// <returns><see langword="true" /> only for a failure to connect raised on this node, never for a status received from a peer.</returns>
    internal static bool IsLocal(ServerCallException exception)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception.StatusCode == FrameStatus.Unavailable &&
               string.Equals(exception.Message, ServerOpContract.OwnerUnreachableDetail, StringComparison.Ordinal) &&
               IsConnectFailure(exception.InnerException);
    }
}
