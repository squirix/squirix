using System.Collections.Concurrent;
using Grpc.Core;

namespace Squirix.Server.Utils;

/// <summary>Rents and returns outbound gRPC <see cref="Metadata" /> instances for interceptor header attachment.</summary>
internal static class GrpcMetadataPool
{
    private static readonly ConcurrentBag<Metadata> Pool = [];

    /// <summary>Rents a cleared <see cref="Metadata" /> instance from the pool, or allocates when empty.</summary>
    /// <returns>A reusable metadata bag owned by the caller until <see cref="Return" />.</returns>
    internal static Metadata Rent()
    {
        if (!Pool.TryTake(out var metadata))
            return [];
        metadata.Clear();
        return metadata;
    }

    /// <summary>Clears and returns a rented <see cref="Metadata" /> instance to the pool.</summary>
    /// <param name="metadata">Rented metadata, or <see langword="null" /> when nothing was rented.</param>
    internal static void Return(Metadata? metadata)
    {
        if (metadata is null)
            return;

        metadata.Clear();
        Pool.Add(metadata);
    }
}
