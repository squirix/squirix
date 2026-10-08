using System;
using System.Globalization;
using Grpc.Core;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Adapters.Grpc;

/// <summary>Reads the refusals that name a stale route to a group leader, and builds the refusal of an operation whose single reroute was spent.</summary>
/// <remarks>
/// A stale route is refused with <see cref="StatusCode.FailedPrecondition" /> before anything is appended: either the stale-owner error code,
/// or the stale-term detail or error code. The refusing node may name the leader it knows in the optional leader hint trailers.
/// </remarks>
internal static class StaleRouteSignals
{
    /// <summary>The stable detail of the refusal of an operation whose route went stale again after its single reroute.</summary>
    internal const string LeaderChangedDetail = "replica_leader_changed";

    /// <summary>Trailer key that names the node the refusing node knows as the leader of the group.</summary>
    internal const string LeaderNodeIdMetadataKey = "squirix-leader-node-id";

    /// <summary>Trailer key that carries the term of the named leader, in invariant decimal form.</summary>
    internal const string LeaderTermMetadataKey = "squirix-leader-term";

    private const string StaleOwnerErrorCode = "stale-owner";

    /// <summary>Creates the refusal of an operation whose route went stale after its single reroute, or that found no other route.</summary>
    /// <returns><see cref="StatusCode.Unavailable" /> with <see cref="LeaderChangedDetail" />: nothing was written, so a retry with the same operation id is safe.</returns>
    internal static RpcException LeaderChanged() => new(new Status(StatusCode.Unavailable, LeaderChangedDetail));

    /// <summary>Determines whether a failure refuses a stale route, and reads the leader the refusing node named.</summary>
    /// <param name="exception">The failure of one attempt.</param>
    /// <param name="hint">The named leader and its term (zero when the term is absent or unreadable); <see langword="default" /> when none is named.</param>
    /// <returns><see langword="true" /> for a stale-owner or stale-term refusal; <see langword="false" /> for every other failure.</returns>
    internal static bool TryReadStale(RpcException exception, out LeaderRoute hint)
    {
        ArgumentNullException.ThrowIfNull(exception);
        hint = default;
        if (exception.StatusCode != StatusCode.FailedPrecondition)
            return false;

        var code = exception.Trailers.GetValue(GrpcStaleOwnerMarkers.ErrorCodeMetadataKey);
        var stale = string.Equals(code, StaleOwnerErrorCode, StringComparison.Ordinal) || string.Equals(code, RefusalCodes.StaleTerm, StringComparison.Ordinal) ||
                    StaleTermClassifier.Classify(exception) == StaleTermVerdict.Stale;
        if (!stale)
            return false;

        var leader = exception.Trailers.GetValue(LeaderNodeIdMetadataKey);
        if (string.IsNullOrEmpty(leader))
            return true;

        _ = ulong.TryParse(exception.Trailers.GetValue(LeaderTermMetadataKey), NumberStyles.None, CultureInfo.InvariantCulture, out var term);
        hint = new LeaderRoute(leader, term);
        return true;
    }
}
