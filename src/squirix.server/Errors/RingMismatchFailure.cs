using System;
using Grpc.Core;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Errors;

/// <summary>Builds and classifies the failures that refuse cache operations on a cluster ring mismatch.</summary>
internal static class RingMismatchFailure
{
    private const string FencedErrorCode = "ring-fenced";

    private const string MismatchErrorCode = "ring-mismatch";

    /// <summary>Creates the refusal of a forwarded call whose sender ring differs from the receiver ring.</summary>
    /// <returns><see cref="StatusCode.Unavailable" /> with the ring-mismatch trailer; nothing was executed.</returns>
    internal static RpcException Mismatch() => Create(ServerOpContract.RingMismatchDetail, MismatchErrorCode);

    /// <summary>Creates the refusal of any cache operation on a node that detected a ring mismatch.</summary>
    /// <returns><see cref="StatusCode.Unavailable" /> with the ring-fenced trailer; nothing was executed.</returns>
    internal static RpcException Fenced() => Create(ServerOpContract.RingFencedDetail, FencedErrorCode);

    /// <summary>Determines whether <paramref name="exception" /> is the ring-mismatch refusal.</summary>
    /// <param name="exception">The RPC failure to classify.</param>
    /// <returns><see langword="true" /> when the failure is <see cref="Mismatch" />; the ring-fenced refusal is not a mismatch.</returns>
    internal static bool IsMismatch(RpcException exception) => HasErrorCode(exception, MismatchErrorCode);

    /// <summary>Determines whether <paramref name="exception" /> is a ring refusal that a retry cannot fix.</summary>
    /// <param name="exception">The RPC failure to classify.</param>
    /// <returns><see langword="true" /> for the ring-mismatch and ring-fenced refusals.</returns>
    internal static bool IsRefusal(RpcException exception) => HasErrorCode(exception, MismatchErrorCode) || HasErrorCode(exception, FencedErrorCode);

    private static RpcException Create(string detail, string errorCode) => new(
        new Status(StatusCode.Unavailable, detail),
        new Metadata { { GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, errorCode } });

    private static bool HasErrorCode(RpcException exception, string errorCode)
    {
        ArgumentNullException.ThrowIfNull(exception);
        if (exception.StatusCode != StatusCode.Unavailable)
            return false;

        var trailers = exception.Trailers;
        for (var i = 0; i < trailers.Count; i++)
        {
            var entry = trailers[i];
            if (!entry.IsBinary &&
                string.Equals(entry.Key, GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, StringComparison.OrdinalIgnoreCase) &&
                string.Equals(entry.Value, errorCode, StringComparison.Ordinal))
                return true;
        }

        return false;
    }
}
