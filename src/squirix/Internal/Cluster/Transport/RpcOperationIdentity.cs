using System;

namespace Squirix.Internal.Cluster.Transport;

/// <summary>Generates operation identifiers for mutating cache RPCs.</summary>
public static class RpcOperationIdentity
{
    /// <summary>Creates a new unique operation identifier for one logical mutation attempt.</summary>
    /// <returns>A 32-character lowercase hex identifier (UUID without hyphens).</returns>
    public static string New() => Guid.NewGuid().ToString("N");
}
