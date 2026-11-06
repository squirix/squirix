using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Derives deterministic domain-separated identities for replicated expiration tombstones.</summary>
internal static class ReplicaExpirationOperationId
{
    /// <summary>The operation scope reserved for leader expiration tombstones.</summary>
    /// <remarks>
    /// Client mutations use their cache name as the scope. A cache name cannot contain a colon, so no client operation can occupy this
    /// scope or collide with a tombstone identity.
    /// </remarks>
    internal const string OperationScope = "squirix:replicated-expiration";

    private const int StackBufferBytes = 256;

    internal static string Create(string groupId, string cacheName, string key, long version, DateTime expiresUtc)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(groupId);
        ArgumentException.ThrowIfNullOrWhiteSpace(cacheName);
        ArgumentException.ThrowIfNullOrWhiteSpace(key);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(version);
        if (expiresUtc.Kind != DateTimeKind.Utc)
            throw new ArgumentException("Expiration must be an absolute UTC value.", nameof(expiresUtc));

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        var requiredBytes = Math.Max(Encoding.UTF8.GetByteCount(groupId), Encoding.UTF8.GetByteCount(cacheName));
        requiredBytes = Math.Max(requiredBytes, Encoding.UTF8.GetByteCount(key));
        var rented = requiredBytes > StackBufferBytes ? ArrayPool<byte>.Shared.Rent(requiredBytes) : null;
        Span<byte> buffer = stackalloc byte[StackBufferBytes];
        if (rented != null)
            buffer = rented;

        try
        {
            AppendBytes(hash, "squirix:replicated-expiration:v1"u8);
            AppendUtf8(hash, groupId, buffer);
            AppendUtf8(hash, cacheName, buffer);
            AppendUtf8(hash, key, buffer);
            AppendInt64(hash, version);
            AppendInt64(hash, expiresUtc.Ticks);
            var digest = hash.GetHashAndReset();
            return Convert.ToHexStringLower(digest.AsSpan(0, 16));
        }
        finally
        {
            if (rented != null)
                ArrayPool<byte>.Shared.Return(rented, true);
        }
    }

    private static void AppendInt64(IncrementalHash hash, long value)
    {
        Span<byte> valueBytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(valueBytes, value);
        AppendBytes(hash, valueBytes);
    }

    private static void AppendUtf8(IncrementalHash hash, string value, Span<byte> buffer)
    {
        var written = Encoding.UTF8.GetBytes(value, buffer);
        AppendBytes(hash, buffer[..written]);
    }

    private static void AppendBytes(IncrementalHash hash, ReadOnlySpan<byte> value)
    {
        Span<byte> length = stackalloc byte[sizeof(int)];
        BinaryPrimitives.WriteInt32LittleEndian(length, value.Length);
        hash.AppendData(length);
        hash.AppendData(value);
    }
}
