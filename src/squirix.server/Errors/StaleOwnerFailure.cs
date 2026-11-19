using System;
using System.Globalization;
using Squirix.Transport.Binary;
using Squirix.Transport.Grpc.Cache;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Errors;

/// <summary>Builds the failure that refuses an operation on a key owned by another node.</summary>
internal static class StaleOwnerFailure
{
    /// <summary>Creates the stale-owner failure.</summary>
    /// <param name="owner">The node that owns the key.</param>
    /// <param name="self">This node.</param>
    /// <returns><see cref="FrameStatus.FailedPrecondition" /> with the stale-owner trailer.</returns>
    internal static ServerCallException Create(string owner, string self) => CreateRefusal(Detail(owner, self), GrpcStaleOwnerMarkers.StaleOwnerErrorCodeValue, null, 0);

    /// <summary>Creates the stale-owner failure that names the elected leader of the key's group as a hint.</summary>
    /// <param name="leader">The node this node knows as the leader of the group.</param>
    /// <param name="self">This node.</param>
    /// <param name="leaderTerm">The term of that leader.</param>
    /// <returns><see cref="FrameStatus.FailedPrecondition" /> with the stale-owner trailer and the leader hint trailers.</returns>
    internal static ServerCallException Create(string leader, string self, ulong leaderTerm) => CreateRefusal(Detail(leader, self), GrpcStaleOwnerMarkers.StaleOwnerErrorCodeValue, leader, leaderTerm);

    internal static ServerCallException CreateRefusal(string detail, string code, string? leader, ulong term)
    {
        var error = new BinaryCallError { Detail = detail };
        error.Trailers.Add(new BinaryCallError.Types.Trailer { Key = GrpcStaleOwnerMarkers.ErrorCodeMetadataKey, Text = code });
        if (!string.IsNullOrEmpty(leader) && !leader.AsSpan().ContainsAnyExceptInRange(' ', '~'))
        {
            error.Trailers.Add(new BinaryCallError.Types.Trailer { Key = GrpcStaleOwnerMarkers.LeaderNodeIdMetadataKey, Text = leader });
            error.Trailers.Add(new BinaryCallError.Types.Trailer { Key = GrpcStaleOwnerMarkers.LeaderTermMetadataKey, Text = term.ToString(CultureInfo.InvariantCulture) });
        }

        return new ServerCallException(FrameStatus.FailedPrecondition, error);
    }

    private static string Detail(string owner, string self) => $"Key is owned by '{owner}', not current node '{self}'.";
}
