using System;
using System.Buffers;
using System.Security.Cryptography;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Builds deterministic fingerprints for mutating cache RPC requests.</summary>
internal static class RpcMutationFingerprints
{
    public static string GetOrAdd(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("get-or-add-async", cacheName, key, HashMessage(entry));

    public static string Remove(string cacheName, string key) => JoinFingerprint("remove-async", cacheName, key);

    public static string RemoveExpiration(string cacheName, string key) => JoinFingerprint("remove-expiration-async", cacheName, key);

    public static string SetEntry(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("set-entry-async", cacheName, key, HashMessage(entry));

    public static string Touch(string cacheName, string key, Duration expiration) => JoinFingerprint("touch-async", cacheName, key, HashMessage(expiration));

    public static string TryAddEntry(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("try-add-entry-async", cacheName, key, HashMessage(entry));

    public static string Update(string cacheName, string key, CacheEntryWire entry) => JoinFingerprint("update-async", cacheName, key, HashMessage(entry));

    private static string JoinFingerprint(string separator, params ReadOnlySpan<string?> parts) => string.Join(separator, parts);

    private static string HashMessage(IMessage message)
    {
        var size = message.CalculateSize();
        var buffer = ArrayPool<byte>.Shared.Rent(size);
        try
        {
            message.WriteTo(buffer.AsSpan(0, size));
            Span<byte> digest = stackalloc byte[32];
            _ = SHA256.HashData(buffer.AsSpan(0, size), digest);
            return HexFormat.FormatSha256HexUpper(digest);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
