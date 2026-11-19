using System;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Errors;

/// <summary>Builds and classifies the failures that refuse cache operations on a cluster ring mismatch.</summary>
internal static class RingMismatchFailure
{
    private const string FencedErrorCode = "ring-fenced";

    private const string MismatchErrorCode = "ring-mismatch";

    /// <summary>Creates the refusal of a forwarded call whose sender ring differs from the receiver ring.</summary>
    /// <returns><see cref="FrameStatus.Unavailable" /> with the ring-mismatch trailer; nothing was executed.</returns>
    internal static ServerCallException Mismatch() => Create(ServerOpContract.RingMismatchDetail, MismatchErrorCode);

    /// <summary>Creates the refusal of any cache operation on a node that detected a ring mismatch.</summary>
    /// <returns><see cref="FrameStatus.Unavailable" /> with the ring-fenced trailer; nothing was executed.</returns>
    internal static ServerCallException Fenced() => Create(ServerOpContract.RingFencedDetail, FencedErrorCode);

    /// <summary>Determines whether <paramref name="exception" /> is the ring-mismatch refusal.</summary>
    /// <param name="exception">The RPC failure to classify.</param>
    /// <returns><see langword="true" /> when the failure is <see cref="Mismatch" />; the ring-fenced refusal is not a mismatch.</returns>
    internal static bool IsMismatch(ServerCallException exception) => HasErrorCode(exception, MismatchErrorCode);

    /// <summary>Determines whether <paramref name="exception" /> is a ring refusal that a retry cannot fix.</summary>
    /// <param name="exception">The RPC failure to classify.</param>
    /// <returns><see langword="true" /> for the ring-mismatch and ring-fenced refusals.</returns>
    internal static bool IsRefusal(ServerCallException exception) => HasErrorCode(exception, MismatchErrorCode) || HasErrorCode(exception, FencedErrorCode);

    private static ServerCallException Create(string detail, string errorCode)
    {
        var failure = new ServerCallException(FrameStatus.Unavailable, detail);
        failure.Error.Trailers.Add(new BinaryCallError.Types.Trailer { Key = GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, Text = errorCode });
        return failure;
    }

    private static bool HasErrorCode(ServerCallException exception, string errorCode)
    {
        ArgumentNullException.ThrowIfNull(exception);
        return exception.StatusCode == FrameStatus.Unavailable && string.Equals(exception.FindTrailer(GrpcStaleOwnerMarkers.ErrorCodeMetadataKey), errorCode, StringComparison.Ordinal);
    }
}
