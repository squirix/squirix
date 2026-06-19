using System;
using System.Security.Cryptography;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>Builds deterministic fingerprints for mutating cache RPC requests.</summary>
internal static class RpcMutationFingerprints
{
    public static string SetEntry(string cacheName, string key, CacheEntryWire entry) => Concat("set-entry-async", cacheName, key, HashMessage(entry));

    public static string TryAddEntry(string cacheName, string key, CacheEntryWire entry) => Concat("try-add-entry-async", cacheName, key, HashMessage(entry));

    public static string Remove(string cacheName, string key) => Concat("remove-async", cacheName, key);

    public static string Touch(string cacheName, string key, Duration expiration) => Concat("touch-async", cacheName, key, HashMessage(expiration));

    public static string RemoveExpiration(string cacheName, string key) => Concat("remove-expiration-async", cacheName, key);

    public static string Update(string cacheName, string key, CacheEntryWire entry) => Concat("update-async", cacheName, key, HashMessage(entry));

    public static string GetOrAdd(string cacheName, string key, CacheEntryWire entry) => Concat("get-or-add-async", cacheName, key, HashMessage(entry));

    private static string Concat(params string[] parts) => string.Join('|', parts);

    private static string HashMessage(IMessage message)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(message.ToByteArray(), digest);
        return HexFormat.FormatSha256HexUpper(digest);
    }
}
