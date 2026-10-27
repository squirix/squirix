using System;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Services;

/// <summary>Computes the operation fingerprints of replicated client mutations from the request alone.</summary>
/// <remarks>
/// A fingerprint covers what the client asked for, never what the leader decided, so a retry keeps its identity whatever it finds
/// and whenever it is prepared. The committer computes it without preparing the mutation, to tell a retry of an unresolved entry from
/// an identifier reused with another request.
/// </remarks>
internal static class ReplicaOperationFingerprints
{
    /// <summary>Computes the fingerprint of an expiration tombstone from the expired entry it removes.</summary>
    /// <param name="cacheName">Target cache name.</param>
    /// <param name="key">Target key.</param>
    /// <param name="version">Version of the expired entry.</param>
    /// <param name="deadlineTicks">UTC ticks of the passed deadline of the expired entry.</param>
    /// <returns>The SHA-256 fingerprint bytes.</returns>
    /// <remarks>No client asks for a tombstone, so it has no client operation identifier: its identity is the expired entry itself.</remarks>
    internal static byte[] Expire(string cacheName, string key, long version, long deadlineTicks)
    {
        Span<byte> expired = stackalloc byte[sizeof(long) * 2];
        BinaryPrimitives.WriteInt64LittleEndian(expired, version);
        BinaryPrimitives.WriteInt64LittleEndian(expired[sizeof(long)..], deadlineTicks);
        return Compute(string.Empty, cacheName, key, ReplicaMutationKinds.Expire, expired);
    }

    internal static byte[] Remove(string operationId, string cacheName, string key) => Compute(operationId, cacheName, key, ReplicaMutationKinds.Remove, []);

    internal static byte[] RemoveExpiration(string operationId, string cacheName, string key) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.RemoveExpiration, []);

    internal static byte[] Set(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.Set, entry.MapToProto().ToByteArray());

    internal static byte[] Touch(string operationId, string cacheName, string key, TimeSpan expiration)
    {
        Span<byte> requested = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(requested, expiration.Ticks);
        return Compute(operationId, cacheName, key, ReplicaMutationKinds.Touch, requested);
    }

    internal static byte[] AddIfAbsent(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.TryAdd, entry.MapToProto().ToByteArray());

    internal static byte[] Update(string operationId, string cacheName, string key, object? value) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.Update, ServerProtoEx.CacheValueToGrpcValue(value).ToByteArray());

    private static void Append(IncrementalHash hash, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Span<byte> length = stackalloc byte[4];
        BinaryPrimitives.WriteInt32LittleEndian(length, bytes.Length);
        hash.AppendData(length);
        hash.AppendData(bytes);
    }

    /// <summary>Computes the canonical operation fingerprint of a client mutation, scoped to its cache.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name, which is also the operation scope.</param>
    /// <param name="key">Target key.</param>
    /// <param name="mutationKind">Cache mutation kind.</param>
    /// <param name="mutationPayload">Cache mutation bytes.</param>
    /// <returns>The SHA-256 fingerprint bytes.</returns>
    /// <remarks>
    /// Each string field contributes its 32-bit little-endian UTF-8 byte count followed by the bytes
    /// themselves, in field order, so concatenations that only differ at field boundaries hash
    /// differently.
    /// </remarks>
    private static byte[] Compute(string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> mutationPayload)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Append(hash, cacheName);
        Append(hash, operationId);
        Append(hash, cacheName);
        Append(hash, key);
        Append(hash, mutationKind);
        hash.AppendData(mutationPayload);
        return hash.GetHashAndReset();
    }
}
