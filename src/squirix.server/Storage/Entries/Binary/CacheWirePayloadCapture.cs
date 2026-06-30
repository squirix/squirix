using System;
using System.Text.Json;

namespace Squirix.Server.Storage.Entries.Binary;

/// <summary>Captures binary gRPC value payloads for server-side Get wire caching.</summary>
internal static class CacheWirePayloadCapture
{
    internal static byte[]? CaptureForStore(object? value)
    {
        if (UsesDirectGrpcScalar(value))
            return null;

        return CacheEntryCodec.EncodeWireValueToOwned(value);
    }

    internal static byte[]? CopyFromEntryWire(ReadOnlySpan<byte> payload)
    {
        if (payload.IsEmpty)
            return null;

#pragma warning disable ZA0302
        var owned = new byte[payload.Length];
#pragma warning restore ZA0302
        payload.CopyTo(owned);
        return owned;
    }

    internal static byte[]? ResolveForStore(object? value, byte[]? ingressWirePayload) =>
        ingressWirePayload ?? CaptureForStore(value);

    private static bool UsesDirectGrpcScalar(object? value) => value switch
    {
        null or bool or string or int or long or double => true,
        JsonElement json => UsesDirectGrpcScalar(json),
        _ => false,
    };

    private static bool UsesDirectGrpcScalar(JsonElement json) => json.ValueKind switch
    {
        JsonValueKind.Null or JsonValueKind.String or JsonValueKind.True or JsonValueKind.False => true,
        JsonValueKind.Number when json.TryGetInt64(out _) => true,
        _ => false,
    };
}
