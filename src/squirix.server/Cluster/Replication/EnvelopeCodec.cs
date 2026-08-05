using System;
using System.Buffers.Binary;
using System.Text;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Versioned binary codec for closed replication envelope headers.</summary>
internal static class EnvelopeCodec
{
    /// <summary>Gets the durable/network schema version encoded by this codec.</summary>
    internal const uint SchemaVersion = 1;

    private const string FixedHeaderValidationMessage = "Replication envelope payload is truncated.";
    private const int FixedHeaderByteCount = 4 + 4 + 8 + 8 + 8;

    /// <summary>Encodes a replication envelope into a versioned binary buffer.</summary>
    /// <param name="envelope">Envelope fields to encode.</param>
    /// <returns>Owned encoded bytes.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="envelope" /> is null.</exception>
    internal static byte[] Encode(Envelope envelope)
    {
        ArgumentNullException.ThrowIfNull(envelope);

        var groupIdBytes = Encoding.UTF8.GetBytes(envelope.GroupId);
        var leaderBytes = Encoding.UTF8.GetBytes(envelope.LeaderNodeId);
        var senderBytes = Encoding.UTF8.GetBytes(envelope.SenderNodeId);
        var fingerprint = envelope.TopologyFingerprint;

        var length = FixedHeaderByteCount + 4 + groupIdBytes.Length + 4 + fingerprint.Length + 4 + leaderBytes.Length + 4 + senderBytes.Length + 8;

        var owned = OwnedBufferKit.Owned(length);
        var buffer = owned.AsSpan();
        var offset = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], SchemaVersion);
        offset += 4;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], envelope.PayloadChecksum);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], envelope.ConfigurationGeneration);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], envelope.Term);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], envelope.LogIndex);
        offset += 8;
        WriteBytes(buffer, groupIdBytes, ref offset);
        WriteBytes(buffer, fingerprint.Span, ref offset);
        WriteBytes(buffer, leaderBytes, ref offset);
        WriteBytes(buffer, senderBytes, ref offset);
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], envelope.CommitIndex);
        return owned;
    }

    /// <summary>Decodes a replication envelope from a versioned binary buffer.</summary>
    /// <param name="payload">Encoded envelope bytes.</param>
    /// <returns>Decoded envelope.</returns>
    /// <exception cref="ArgumentException">Thrown when the payload is truncated or the schema is unsupported.</exception>
    internal static Envelope Decode(ReadOnlySpan<byte> payload)
    {
        if (payload.Length < FixedHeaderByteCount + 16)
            throw new ArgumentException(FixedHeaderValidationMessage, nameof(payload));

        var offset = 0;
        var header = ReadFixedHeader(payload, ref offset);

        var groupId = ReadString(payload, ref offset);
        var fingerprint = ReadOwnedBytes(payload, ref offset);
        var leaderNodeId = ReadString(payload, ref offset);
        var senderNodeId = ReadString(payload, ref offset);
        if (payload.Length - offset < 8)
            throw new ArgumentException("Replication envelope payload is truncated.", nameof(payload));

        var commitIndex = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        return new Envelope(header.SchemaVersion, groupId, fingerprint, header.ConfigurationGeneration, header.Term, leaderNodeId, senderNodeId, header.LogIndex, commitIndex, header.PayloadChecksum);
    }

    private static (uint SchemaVersion, uint PayloadChecksum, ulong ConfigurationGeneration, ulong Term, ulong LogIndex) ReadFixedHeader(ReadOnlySpan<byte> payload, ref int offset)
    {
        var schema = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        offset += 4;
        if (schema != SchemaVersion)
            throw new ArgumentException($"Unsupported replication envelope schema version '{schema}'.", nameof(payload));

        var checksum = BinaryPrimitives.ReadUInt32LittleEndian(payload[offset..]);
        offset += 4;
        var generation = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;
        var term = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;
        var logIndex = BinaryPrimitives.ReadUInt64LittleEndian(payload[offset..]);
        offset += 8;
        return (schema, checksum, generation, term, logIndex);
    }

    private static void WriteBytes(Span<byte> buffer, ReadOnlySpan<byte> value, ref int offset)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], value.Length);
        offset += 4;
        value.CopyTo(buffer[offset..]);
        offset += value.Length;
    }

    private static byte[] ReadOwnedBytes(ReadOnlySpan<byte> payload, ref int offset)
    {
        if (payload.Length - offset < 4)
            throw new ArgumentException(FixedHeaderValidationMessage, nameof(payload));

        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
        offset += 4;
        if (length < 0 || length > payload.Length - offset)
            throw new ArgumentException(FixedHeaderValidationMessage, nameof(payload));

        var bytes = OwnedBufferKit.CopyToOwned(payload.Slice(offset, length));
        offset += length;
        return bytes;
    }

    private static string ReadString(ReadOnlySpan<byte> payload, ref int offset)
    {
        if (payload.Length - offset < 4)
            throw new ArgumentException(FixedHeaderValidationMessage, nameof(payload));
        var length = BinaryPrimitives.ReadInt32LittleEndian(payload[offset..]);
        offset += 4;
        if (length < 0 || length > payload.Length - offset)
            throw new ArgumentException(FixedHeaderValidationMessage, nameof(payload));
        var value = Encoding.UTF8.GetString(payload.Slice(offset, length));
        offset += length;
        return value;
    }

    /// <summary>
    /// Exact-size owned byte buffer helpers for envelope encoding.
    /// Cluster.Replication may not depend on <c>Squirix.Server.Utils</c> (config.nsdepcop), so the
    /// kernel-owned-buffer escape must be re-hosted here.
    /// </summary>
    private static class OwnedBufferKit
    {
        /// <summary>Allocates an exact-size owned byte buffer that must outlive the current span.</summary>
        /// <param name="length">Exact buffer length.</param>
        /// <returns>An owned byte array of the requested length.</returns>
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the caller fills the buffer and retains ownership.
        internal static byte[] Owned(int length) => new byte[length];
#pragma warning restore ZA0302

        /// <summary>Copies a span into an exact-size owned byte buffer.</summary>
        /// <param name="source">Source bytes to copy.</param>
        /// <returns>An owned byte array containing the source bytes.</returns>
        internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
        {
            var owned = Owned(source.Length);
            source.CopyTo(owned);
            return owned;
        }
    }
}
