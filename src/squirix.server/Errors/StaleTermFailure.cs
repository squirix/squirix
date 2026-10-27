using Grpc.Core;
using Squirix.Transport.Grpc.Mappers;

namespace Squirix.Server.Errors;

/// <summary>Builds the failure that refuses a write on a node whose term a higher term superseded, before anything is appended.</summary>
internal static class StaleTermFailure
{
    /// <summary>Creates the stale-term failure.</summary>
    /// <param name="leaderNodeId">The leader this node knows of the newer term; <see langword="null" /> when none is known.</param>
    /// <param name="leaderTerm">The term of that leader.</param>
    /// <returns>
    /// <see cref="StatusCode.FailedPrecondition" /> with the stale-term detail and error code, and the leader hint when a leader is known:
    /// nothing was written, so the operation may run once more on the current leader.
    /// </returns>
    internal static RpcException Create(string? leaderNodeId, ulong leaderTerm) => new(
        new Status(StatusCode.FailedPrecondition, GrpcStaleOwnerMarkers.StaleTermErrorCodeValue),
        GrpcStaleOwnerMarkers.CreateStaleTermTrailers(leaderNodeId, leaderTerm));
}
