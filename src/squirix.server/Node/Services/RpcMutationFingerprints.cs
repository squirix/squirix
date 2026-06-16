using System;
using System.Security.Cryptography;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Utils;
using Squirix.Transport.Grpc.Cache;

namespace Squirix.Server.Node.Services;

/// <summary>
/// Builds deterministic fingerprints for mutating cache RPC requests.
/// </summary>
internal static class RpcMutationFingerprints
{
    public static string Set(string cacheName, string key, Entry entry) => Concat("set", cacheName, key, HashMessage(entry));

    public static string SetValue(string cacheName, string key, CacheValue value, Timestamp? expiresUtc, Duration? expiration) =>
        Concat("set-value", cacheName, key, HashMessage(value), HashOptionalTimestamp(expiresUtc), HashOptionalDuration(expiration));

    public static string TrySet(string cacheName, string key, Entry entry) => Concat("try-set", cacheName, key, HashMessage(entry));

    public static string TrySetValue(string cacheName, string key, CacheValue value, Timestamp? expiresUtc, Duration? expiration) =>
        Concat("try-set-value", cacheName, key, HashMessage(value), HashOptionalTimestamp(expiresUtc), HashOptionalDuration(expiration));

    public static string Remove(string cacheName, string key) => Concat("remove", cacheName, key);

    public static string Touch(string cacheName, string key, Duration expiration) => Concat("touch", cacheName, key, HashMessage(expiration));

    public static string RemoveExpiration(string cacheName, string key) => Concat("remove-expiration", cacheName, key);

    public static string UpdateValue(string cacheName, string key, CacheValue value) => Concat("update-value", cacheName, key, HashMessage(value));

    public static string GetOrAddValue(string cacheName, string key, CacheValue value, Timestamp? expiresUtc, Duration? expiration) =>
        Concat("get-or-add-value", cacheName, key, HashMessage(value), HashOptionalTimestamp(expiresUtc), HashOptionalDuration(expiration));

    private static string Concat(params string[] parts) => string.Join('|', parts);

    private static string HashMessage(IMessage message)
    {
        Span<byte> digest = stackalloc byte[32];
        _ = SHA256.HashData(message.ToByteArray(), digest);
        return HexFormat.FormatSha256HexUpper(digest);
    }

    private static string HashOptionalDuration(Duration? duration) => duration is null ? "none" : HashMessage(duration);

    private static string HashOptionalTimestamp(Timestamp? timestamp) => timestamp is null ? "none" : HashMessage(timestamp);
}
