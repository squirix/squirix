using System;
using Google.Protobuf;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Proto response serialization for idempotency outcome payloads.</summary>
[Immutable]
internal static class IdempotencyResponseCodec
{
    /// <summary>Serializes a proto response into an exact-size owned buffer.</summary>
    /// <param name="response">The response to serialize.</param>
    /// <returns>Owned response bytes.</returns>
    internal static byte[] SerializeResponseBytes(IMessage response)
    {
        ArgumentNullException.ThrowIfNull(response);

        var size = response.CalculateSize();
        var bytes = BufferEx.Owned(size);
        response.WriteTo(bytes);
        return bytes;
    }
}
