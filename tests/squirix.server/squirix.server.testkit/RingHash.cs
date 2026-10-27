using System;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.TestKit;

/// <summary>Exposes the server ring hash so test and benchmark key-owner helpers stay in step with the server.</summary>
[Immutable]
public static class RingHash
{
    /// <summary>Hashes route-key or virtual-node bytes the way the server ring does.</summary>
    /// <param name="data">The bytes to hash.</param>
    /// <returns>The 64-bit ring position.</returns>
    public static ulong Hash(ReadOnlySpan<byte> data) => XxHash64.Hash(data);
}
