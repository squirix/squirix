using System;
using System.Globalization;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Reads the refusals that name a stale route to a group leader.</summary>
/// <remarks>
/// A stale route is refused with <see cref="FrameStatus.FailedPrecondition" /> before anything is appended: either the stale-owner error code,
/// or the stale-term detail or error code. The refusing node may name the leader it knows in the optional leader hint trailers.
/// </remarks>
internal static class StaleRouteSignals
{
    /// <summary>Determines whether a failure refuses a stale route, and reads the leader the refusing node named.</summary>
    /// <param name="exception">The failure of one attempt.</param>
    /// <param name="hint">The named leader and its term (zero when the term is absent or unreadable); <see langword="default" /> when none is named.</param>
    /// <returns><see langword="true" /> for a stale-owner or stale-term refusal; <see langword="false" /> for every other failure.</returns>
    internal static bool TryReadStale(ServerCallException exception, out LeaderRoute hint)
    {
        ArgumentNullException.ThrowIfNull(exception);
        hint = default;
        if (exception.StatusCode != FrameStatus.FailedPrecondition)
            return false;

        var code = exception.FindTrailer(GrpcStaleOwnerMarkers.ErrorCodeMetadataKey);
        var stale = string.Equals(code, GrpcStaleOwnerMarkers.StaleOwnerErrorCodeValue, StringComparison.Ordinal) ||
                    string.Equals(code, GrpcStaleOwnerMarkers.StaleTermErrorCodeValue, StringComparison.Ordinal) ||
                    StaleTermClassifier.Classify(exception) == StaleTermVerdict.Stale;
        if (!stale)
            return false;

        var leader = exception.FindTrailer(GrpcStaleOwnerMarkers.LeaderNodeIdMetadataKey);
        if (string.IsNullOrEmpty(leader))
            return true;

        _ = ulong.TryParse(exception.FindTrailer(GrpcStaleOwnerMarkers.LeaderTermMetadataKey), NumberStyles.None, CultureInfo.InvariantCulture, out var term);
        hint = new LeaderRoute(leader, term);
        return true;
    }
}
