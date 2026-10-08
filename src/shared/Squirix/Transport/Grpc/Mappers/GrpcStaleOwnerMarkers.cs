using System.Globalization;
using Grpc.Core;

// ReSharper disable once CheckNamespace
namespace Squirix.Transport.Grpc.Mappers;

/// <summary>Stable transport signals for stale-owner and stale-term routing failures on internal cluster gRPC calls.</summary>
internal static class GrpcStaleOwnerMarkers
{
    /// <summary>Trailer key that carries a stable machine-readable error code on internal cluster gRPC calls.</summary>
    internal const string ErrorCodeMetadataKey = "squirix-error-code";

    /// <summary>Trailer key that names the node the refusing node knows as the leader of the group.</summary>
    internal const string LeaderNodeIdMetadataKey = "squirix-leader-node-id";

    /// <summary>Trailer key that carries the term of the named leader, in invariant decimal form.</summary>
    internal const string LeaderTermMetadataKey = "squirix-leader-term";

    /// <summary>The error code and the status detail of a refusal by a node whose term is stale: nothing was appended.</summary>
    internal const string StaleTermErrorCodeValue = "stale-term";

    private const string StaleOwnerErrorCodeValue = "stale-owner";

    internal static Metadata CreateStaleOwnerTrailers() => new() { { ErrorCodeMetadataKey, StaleOwnerErrorCodeValue } };

    /// <summary>Creates the stale-owner trailers with the leader hint.</summary>
    /// <param name="leaderNodeId">The known leader; no hint trailers when <see langword="null" /> or empty.</param>
    /// <param name="leaderTerm">The term of the known leader.</param>
    /// <returns>The trailers.</returns>
    internal static Metadata CreateStaleOwnerTrailers(string? leaderNodeId, ulong leaderTerm) => WithLeaderHint(CreateStaleOwnerTrailers(), leaderNodeId, leaderTerm);

    /// <summary>Creates the stale-term trailers with the leader hint.</summary>
    /// <param name="leaderNodeId">The known leader; no hint trailers when <see langword="null" /> or empty.</param>
    /// <param name="leaderTerm">The term of the known leader.</param>
    /// <returns>The trailers.</returns>
    internal static Metadata CreateStaleTermTrailers(string? leaderNodeId, ulong leaderTerm) =>
        WithLeaderHint(new Metadata { { ErrorCodeMetadataKey, StaleTermErrorCodeValue } }, leaderNodeId, leaderTerm);

    private static Metadata WithLeaderHint(Metadata trailers, string? leaderNodeId, ulong leaderTerm)
    {
        if (string.IsNullOrEmpty(leaderNodeId))
            return trailers;

        trailers.Add(LeaderNodeIdMetadataKey, leaderNodeId);
        trailers.Add(LeaderTermMetadataKey, leaderTerm.ToString(CultureInfo.InvariantCulture));
        return trailers;
    }
}
