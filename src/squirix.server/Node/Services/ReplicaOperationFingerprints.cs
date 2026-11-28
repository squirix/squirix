using System;
using System.Buffers.Binary;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Squirix.Server.Core;
using Squirix.Server.Serialization.Wire;
using Squirix.Server.Utils;
using Squirix.Transport.Wire.Cache;
using Squirix.Transport.Wire.Codec;

namespace Squirix.Server.Node.Services;

/// <summary>Computes the operation fingerprints of replicated client mutations from the request alone.</summary>
/// <remarks>
/// A fingerprint covers what the client asked for, never what the leader decided, so a retry keeps its identity whatever it finds
/// and whenever it is prepared. The committer computes it without preparing the mutation, to tell a retry of an unresolved entry from
/// an identifier reused with another request.
/// </remarks>
internal static class ReplicaOperationFingerprints
{
    /// <summary>The largest canonical request hashed from a stack buffer; larger requests are hashed in chunks.</summary>
    private const int StackBufferBytes = 512;
    private const string FingerprintDomain = "squirix-mutation-v2";

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

    internal static byte[] DistributedAccess(string operationId, string cacheName, string key, bool refresh) =>
        Compute(operationId, cacheName, key, refresh ? ReplicaMutationKinds.DistributedRefresh : ReplicaMutationKinds.DistributedGet, []);

    internal static byte[] Remove(string operationId, string cacheName, string key) => Compute(operationId, cacheName, key, ReplicaMutationKinds.Remove, []);

    internal static byte[] RemoveExpiration(string operationId, string cacheName, string key) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.RemoveExpiration, []);

    internal static byte[] Set(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry)
    {
        if (entry.PendingWrite is not { } write)
            return Compute(operationId, cacheName, key, ReplicaMutationKinds.Set, entry.MapToWire());

        var kind = write.TtlTicks is { } ttl ? string.Create(CultureInfo.InvariantCulture, $"set-distributed-ttl:{ttl}") : "set-distributed";
        return Compute(operationId, cacheName, key, kind, write.Envelope);
    }

    internal static byte[] Touch(string operationId, string cacheName, string key, TimeSpan expiration)
    {
        Span<byte> requested = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(requested, expiration.Ticks);
        return Compute(operationId, cacheName, key, ReplicaMutationKinds.Touch, requested);
    }

    internal static byte[] AddIfAbsent(string operationId, string cacheName, string key, NodeCacheEntry<StoredValue> entry) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.TryAdd, entry.MapToWire());

    internal static byte[] Update(string operationId, string cacheName, string key, in StoredValue value) =>
        Compute(operationId, cacheName, key, ReplicaMutationKinds.Update, ServerWireMapping.CacheValueToWire(value));

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
    private static byte[] Compute(string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> mutationPayload) =>
        Compute(operationId, cacheName, key, mutationKind, mutationPayload, default(CacheEntryWire), false);

    private static byte[] Compute<T>(string operationId, string cacheName, string key, string mutationKind, ReadOnlySpan<byte> mutationPayload, T message, bool encodeMessage)
        where T : IWireBody
    {
        var measure = default(WireWriter);
        if (encodeMessage)
            message.Encode(ref measure);
        var payloadLength = encodeMessage ? measure.Written : mutationPayload.Length;
        var length = checked(FieldBytes(FingerprintDomain) + FieldBytes(cacheName) + FieldBytes(operationId) + FieldBytes(key) + FieldBytes(mutationKind) + payloadLength);
        if (length <= StackBufferBytes)
        {
            Span<byte> buffer = stackalloc byte[length];
            var written = WriteRequestFields(buffer, operationId, cacheName, key, mutationKind);
            if (!encodeMessage)
            {
                mutationPayload.CopyTo(buffer[written..]);
            }
            else
            {
                var encoder = new WireWriter(buffer[written..]);
                message.Encode(ref encoder);
            }

            return SHA256.HashData(buffer);
        }

        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        using var writer = new IncrementalHashBufferWriter(hash);
        AppendField(writer, FingerprintDomain);
        AppendField(writer, operationId);
        AppendField(writer, cacheName);
        AppendField(writer, key);
        AppendField(writer, mutationKind);
        if (!encodeMessage)
        {
            hash.AppendData(mutationPayload);
        }
        else
        {
            var encoder = new WireWriter(writer);
            message.Encode(ref encoder);
        }

        return hash.GetHashAndReset();
    }

    /// <summary>Computes the canonical operation fingerprint, writing the native binary payload directly after its request fields.</summary>
    /// <param name="operationId">Client operation identifier.</param>
    /// <param name="cacheName">Target cache name, which is also the operation scope.</param>
    /// <param name="key">Target key.</param>
    /// <param name="mutationKind">Cache mutation kind.</param>
    /// <typeparam name="T">The canonical wire body.</typeparam>
    /// <param name="mutationPayload">The message whose serialized bytes are the cache mutation bytes.</param>
    /// <returns>The SHA-256 fingerprint bytes, equal to those of the serialized message passed to the span overload.</returns>
    private static byte[] Compute<T>(string operationId, string cacheName, string key, string mutationKind, T mutationPayload)
        where T : IWireBody =>
        Compute(operationId, cacheName, key, mutationKind, [], mutationPayload, true);

    private static void AppendField(IncrementalHashBufferWriter writer, string value)
    {
        var destination = writer.GetSpan();
        var byteCount = Encoding.UTF8.GetByteCount(value);
        if (byteCount <= destination.Length - sizeof(int))
        {
            writer.Advance(WriteField(destination, value));
            return;
        }

        BinaryPrimitives.WriteInt32LittleEndian(destination, byteCount);
        writer.Advance(sizeof(int));
        var encoder = Encoding.UTF8.GetEncoder();
        var remaining = value.AsSpan();
        bool completed;
        do
        {
            encoder.Convert(remaining, writer.GetSpan(), true, out var charsUsed, out var bytesUsed, out completed);
            writer.Advance(bytesUsed);
            remaining = remaining[charsUsed..];
        }
        while (!completed);
    }

    private static int WriteRequestFields(Span<byte> buffer, string operationId, string cacheName, string key, string mutationKind)
    {
        var written = WriteField(buffer, FingerprintDomain);
        written += WriteField(buffer[written..], operationId);
        written += WriteField(buffer[written..], cacheName);
        written += WriteField(buffer[written..], key);
        written += WriteField(buffer[written..], mutationKind);
        return written;
    }

    private static int FieldBytes(string value) => checked(sizeof(int) + Encoding.UTF8.GetByteCount(value));

    private static int WriteField(Span<byte> buffer, string value)
    {
        var written = Encoding.UTF8.GetBytes(value, buffer[sizeof(int)..]);
        BinaryPrimitives.WriteInt32LittleEndian(buffer, written);
        return sizeof(int) + written;
    }
}
