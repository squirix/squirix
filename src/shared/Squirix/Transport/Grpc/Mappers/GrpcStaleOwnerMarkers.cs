using System;
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

    /// <summary>The error code of a refusal by a node that neither owns nor leads the key's group: nothing was appended.</summary>
    internal const string StaleOwnerErrorCodeValue = "stale-owner";

    internal static Metadata CreateStaleOwnerTrailers() => new() { { ErrorCodeMetadataKey, StaleOwnerErrorCodeValue } };

    /// <summary>Creates the stale-owner trailers with the leader hint.</summary>
    /// <param name="leaderNodeId">The known leader; no hint trailers when <see langword="null" />, empty, or not printable ASCII.</param>
    /// <param name="leaderTerm">The term of the known leader.</param>
    /// <returns>The trailers.</returns>
    internal static Metadata CreateStaleOwnerTrailers(string? leaderNodeId, ulong leaderTerm) => WithLeaderHint(CreateStaleOwnerTrailers(), leaderNodeId, leaderTerm);

    /// <summary>Creates the stale-term trailers with the leader hint.</summary>
    /// <param name="leaderNodeId">The known leader; no hint trailers when <see langword="null" />, empty, or not printable ASCII.</param>
    /// <param name="leaderTerm">The term of the known leader.</param>
    /// <returns>The trailers.</returns>
    internal static Metadata CreateStaleTermTrailers(string? leaderNodeId, ulong leaderTerm) =>
        WithLeaderHint(new Metadata { { ErrorCodeMetadataKey, StaleTermErrorCodeValue } }, leaderNodeId, leaderTerm);

    /// <summary>Adds the leader hint to refusal trailers when the node identifier can travel as an ASCII metadata value.</summary>
    /// <param name="trailers">The refusal trailers.</param>
    /// <param name="leaderNodeId">The known leader.</param>
    /// <param name="leaderTerm">The term of the known leader.</param>
    /// <returns><paramref name="trailers" />.</returns>
    /// <remarks>
    /// Node identifiers are not restricted to ASCII by configuration. An identifier with a character outside printable ASCII would break
    /// the response, and the refusal marker with it, so the hint is left out and the refusal stays as it is.
    /// </remarks>
    private static Metadata WithLeaderHint(Metadata trailers, string? leaderNodeId, ulong leaderTerm)
    {
        if (string.IsNullOrEmpty(leaderNodeId) || leaderNodeId.AsSpan().ContainsAnyExceptInRange(' ', '~'))
            return trailers;

        trailers.Add(LeaderNodeIdMetadataKey, leaderNodeId);
        trailers.Add(LeaderTermMetadataKey, leaderTerm.ToString(CultureInfo.InvariantCulture));
        return trailers;
    }
}
