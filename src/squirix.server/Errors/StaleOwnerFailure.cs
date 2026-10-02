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
        new Status(StatusCode.FailedPrecondition, $"Key is owned by '{owner}', not current node '{self}'."),
        GrpcStaleOwnerMarkers.CreateStaleOwnerTrailers());
}
