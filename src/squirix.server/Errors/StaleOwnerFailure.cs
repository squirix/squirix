using Grpc.Core;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Errors;

/// <summary>Builds the failure that refuses an operation on a key owned by another node.</summary>
internal static class StaleOwnerFailure
{
    /// <summary>Creates the stale-owner failure.</summary>
    /// <param name="owner">The node that owns the key.</param>
    /// <param name="self">This node.</param>
    /// <returns><see cref="StatusCode.FailedPrecondition" /> with the stale-owner trailer.</returns>
    internal static RpcException Create(string owner, string self) => new(
        new Status(StatusCode.FailedPrecondition, Detail(owner, self)),
        GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers());

    /// <summary>Creates the stale-owner failure that names the elected leader of the key's group as a hint.</summary>
    /// <param name="leader">The node this node knows as the leader of the group.</param>
    /// <param name="self">This node.</param>
    /// <param name="leaderTerm">The term of that leader.</param>
    /// <returns><see cref="StatusCode.FailedPrecondition" /> with the stale-owner trailer and the leader hint trailers.</returns>
    internal static RpcException Create(string leader, string self, ulong leaderTerm) => new(
        new Status(StatusCode.FailedPrecondition, Detail(leader, self)),
        GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers(leader, leaderTerm));

    private static string Detail(string owner, string self) => $"Key is owned by '{owner}', not current node '{self}'.";
}
