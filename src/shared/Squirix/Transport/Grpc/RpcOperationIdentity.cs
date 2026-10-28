using System;
using System.Security.Cryptography;

namespace Squirix.Transport.Grpc;

/// <summary>Generates operation identifiers for mutating cache RPCs.</summary>
public static class RpcOperationIdentity
{
    private const int IdentityByteCount = 16;
    private const int BufferByteCount = 4096;

    /// <summary>
    /// Per-thread block of CSPRNG bytes filled in bulk, so most identifiers need no RNG call. Each identifier takes 16
    /// fresh bytes (128 bits) and the bytes are never reused.
    /// </summary>
    [ThreadStatic]
    private static byte[]? _randomBuffer;

    /// <summary>Offset of the next unused byte in <see cref="_randomBuffer" />.</summary>
    [ThreadStatic]
    private static int _randomOffset;

    /// <summary>Creates a new unique operation identifier for one logical mutation attempt.</summary>
    /// <returns>A 32-character lowercase hex identifier (UUID without hyphens).</returns>
    public static string New()
    {
        var buffer = _randomBuffer ??= new byte[BufferByteCount];
        var offset = _randomOffset;
        if (offset == 0 || offset + IdentityByteCount > BufferByteCount)
        {
            RandomNumberGenerator.Fill(buffer);
            offset = 0;
        }

        var identity = Convert.ToHexStringLower(buffer.AsSpan(offset, IdentityByteCount));
        _randomOffset = offset + IdentityByteCount;
        return identity;
    }
}
