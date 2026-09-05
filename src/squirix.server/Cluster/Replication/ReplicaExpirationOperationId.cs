using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Derives deterministic domain-separated identities for replicated expiration tombstones.</summary>
internal static class ReplicaExpirationOperationId
{
    internal const string OperationScope = "replicated-expiration";

    private const string Domain = "squirix:replicated-expiration:v1";
    private const string HexAlphabet = "0123456789abcdef";

    internal static string Create(string groupId, string cacheName, string key, long version, DateTime expiresUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        if (expiresUtc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Expiration must be an absolute UTC value.", nameof(expiresUtc));

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        AppendUtf8(hash, Domain);
        AppendUtf8(hash, groupId);
        AppendUtf8(hash, cacheName);
        AppendUtf8(hash, key);
        AppendInt64(hash, version);
        AppendInt64(hash, expiresUtc.Ticks);
        var digest = hash.GetHashAndReset();
        return string.Create(
            32,
            digest,
            static (destination, bytes) =>
            {
                for (var index = 0; index < 16; index++)
                {
                    destination[index * 2] = HexAlphabet[bytes[index] >> 4];
                    destination[(index * 2) + 1] = HexAlphabet[bytes[index] & 0x0f];
                }
            });
    }

    private static void AppendInt64(IncrementalHash hash, long value)
    {
        Span<byte> valueBytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(valueBytes, value);
        AppendBytes(hash, valueBytes);
    }

    private static void AppendUtf8(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        AppendBytes(hash, bytes);
    }

    private static void AppendBytes(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
        hash.AppendData(length);
        hash.AppendData(value);
    }
}
