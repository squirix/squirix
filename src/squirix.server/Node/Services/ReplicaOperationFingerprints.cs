using System;
using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;
using Google.Protobuf;
using Squirix.Server.Core;
using Squirix.Server.Serialization.Grpc;

namespace Squirix.Server.Node.Services;

/// <summary>Computes the operation fingerprints of replicated client mutations from the request alone.</summary>
/// <remarks>
/// A fingerprint covers what the client asked for, never what the leader decided, so a retry keeps its identity whatever it finds
/// and whenever it is prepared. The committer computes it without preparing the mutation, to tell a retry of an unresolved entry from
/// an identifier reused with another request.
/// </remarks>
internal static class ReplicaOperationFingerprints
{
    /// <summary>The largest canonical request hashed from a stack buffer; a larger one uses an owned slice of a pooled buffer.</summary>
    private const int StackBufferBytes = 512;

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
        Compute(operationId, cacheName, key, ReplicaMutationKinds.Set, entry.MapToProto());

    internal static byte[] Touch(string operationId, string cacheName, string key, TimeSpan expiration)
    {
        Span<byte> requested = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(requested, expiration.Ticks);
        return Compute(operationId, cacheName, key, ReplicaMutationKinds.Touch, requested);
    }

    internal static byte[] AddIfAbsent(string operationId, string cacheName, string key, NodeCacheEntry<object?> entry) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.TryAdd, entry.MapToProto());

    internal static byte[] Update(string operationId, string cacheName, string key, object? value) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.Update, ServerProtoEx.CacheValueToGrpcValue(value));

    /// <summary>Computes the canonical operation fingerprint of a client mutation, scoped to its cache.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name, which is also the operation scope.</param>
    /// <param name="key">Target key.</param>
    /// <param name="mutationKind">Cache mutation kind.</param>
    /// <param name="mutationPayload">Cache mutation bytes.</param>
    /// <param name="message">The protobuf payload instead of <paramref name="mutationPayload" />, when supplied.</param>
    /// <returns>The SHA-256 fingerprint bytes.</returns>
    /// <remarks>
    /// Each string field contributes its 32-bit little-endian UTF-8 byte count followed by the bytes
    /// themselves, in field order, so concatenations that only differ at field boundaries hash
    /// differently.
    /// </remarks>
    private static byte[] Compute(string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> mutationPayload, IMessage? message = null)
    {
        var length = checked((FieldBytes(cacheName) * 2) + FieldBytes(operationId) + FieldBytes(key) + FieldBytes(mutationKind)
                             + (message?.CalculateSize() ?? mutationPayload.Length));
        if (length <= StackBufferBytes)
        {
            Span<byte> buffer = stackalloc byte[length];
            return HashRequest(buffer, operationId, cacheName, key, mutationKind, mutationPayload, message);
        }

        var rented = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            return HashRequest(rented.AsSpan(0, length), operationId, cacheName, key, mutationKind, mutationPayload, message);
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(rented);
        }
    }

    /// <summary>Computes the canonical operation fingerprint, writing the protobuf payload directly after its request fields.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name, which is also the operation scope.</param>
    /// <param name="key">Target key.</param>
    /// <param name="mutationKind">Cache mutation kind.</param>
    /// <param name="mutationPayload">The message whose serialized bytes are the cache mutation bytes.</param>
    /// <returns>The SHA-256 fingerprint bytes, equal to those of the serialized message passed to the span overload.</returns>
    private static byte[] Compute(string operationId, string cacheName, string key, string mutationKind, IMessage mutationPayload) =>
        Compute(operationId, cacheName, key, mutationKind, [], mutationPayload);

    private static byte[] HashRequest(Span<byte> buffer, string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> payload, IMessage? message)
    {
        var written = WriteField(buffer, cacheName);
        written += WriteField(buffer[written..], operationId);
        written += WriteField(buffer[written..], cacheName);
        written += WriteField(buffer[written..], key);
        written += WriteField(buffer[written..], mutationKind);
        if (message == null)
            payload.CopyTo(buffer[written..]);
        else
            message.WriteTo(buffer[written..]);

        return SHA256.HashData(buffer);
    }

    private static int FieldBytes(string value) => checked(sizeof(int) + Encoding.UTF8.GetByteCount(value));

    private static int WriteField(Span<byte> buffer, string value)
    {
        var written = Encoding.UTF8.GetBytes(value, buffer[sizeof(int)..]);
        BinaryPrimitives.WriteInt32LittleEndian(buffer, written);
        return sizeof(int) + written;
    }
}
