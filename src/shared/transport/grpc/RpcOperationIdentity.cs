using System;

namespace Squirix.Transport.Grpc;

/// <summary>Generates operation identifiers for mutating cache RPCs.</summary>
public static class RpcOperationIdentity
{
    /// <summary>Creates a new unique operation identifier for one logical mutation attempt.</summary>
    /// <returns>A 32-character lowercase hex identifier.</returns>
    public static string New() => Guid.NewGuid().ToString("N");
}
