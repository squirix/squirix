using System;
using System.Buffers.Binary;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Binary codec for replica-group metadata and log frames with CRC32C integrity.</summary>
/// <remarks>
/// <para>On-disk layout is little-endian throughout.</para>
/// <para>Metadata file: <c>magic(4) | version(1) | payload | crc32c(payload)(4)</c> where payload carries the
/// fixed-width term/commit fields followed by length-prefixed UTF-8 strings and the topology fingerprint.</para>
/// <para>Log frame: <c>bodyLength(4) | body | crc32c(body)(4)</c> where body carries
/// <c>logIndex(8) | term(8) | payloadLength(4) | payload</c>. Frames are append-only; a torn tail frame is
/// detected by truncation or CRC mismatch.</para>
/// </remarks>
internal static class GroupLogCodec
{
    /// <summary>Metadata file magic bytes, <c>"SQRM"</c>.</summary>
    private const uint MetaMagic = 0x4D525147u;

    /// <summary>Metadata format version.</summary>
    private const byte MetaVersion = 1;

    /// <summary>Fixed metadata size: magic(4) + version(1) + five ulongs.</summary>
    private const int MetaFixedByteCount = 4 + 1 + (8 * 5);

    /// <summary>Log frame magic bytes, <c>"SQRL"</c>.</summary>
    private const uint FrameMagic = 0x4C525147u;

    /// <summary>Log format version.</summary>
    private const byte FrameVersion = 1;

    private const int FrameHeaderByteCount = 4 + 1;
    private const int FrameFixedByteCount = 8 + 8 + 4;

    /// <summary>Gets the file header bytes written at the start of a replica-group log file.</summary>
    internal static ReadOnlySpan<byte> LogFileHeader =>
    [
        0x53, 0x51, 0x52, 0x4C, // "SQRL"
        0x01, // version 1
    ];

    /// <summary>Computes the encoded length of a metadata payload.</summary>
    /// <param name="meta">The metadata to encode.</param>
    /// <returns>The encoded metadata length in bytes.</returns>
    internal static int ComputeMetaEncodedLength(GroupLogMetadata meta)
    {
        ArgumentNullException.ThrowIfNull(meta.GroupId);
        var groupBytes = Encoding.UTF8.GetByteCount(meta.GroupId);
        var votedBytes = Encoding.UTF8.GetByteCount(meta.VotedFor);
        return MetaFixedByteCount + 4 + groupBytes + 4 + meta.TopologyFingerprint.Length + 4 + votedBytes + 4;
    }

    /// <summary>Encodes metadata into a caller-provided buffer of at least <see cref="ComputeMetaEncodedLength" /> bytes.</summary>
    /// <param name="meta">The metadata to encode.</param>
    /// <param name="buffer">The destination buffer.</param>
    internal static void EncodeMeta(GroupLogMetadata meta, Span<byte> buffer)
    {
        ArgumentNullException.ThrowIfNull(meta.GroupId);
        ArgumentNullException.ThrowIfNull(meta.VotedFor);

        var offset = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], MetaMagic);
        offset += 4;
        buffer[offset] = MetaVersion;
        offset++;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.ConfigurationGeneration);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.CurrentTerm);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.LastLogIndex);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.CommitIndex);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.LastAppliedIndex);
        offset += 8;

        WriteString(buffer, meta.GroupId, ref offset);
        WriteBytes(buffer, meta.TopologyFingerprint.Span, ref offset);
        WriteString(buffer, meta.VotedFor, ref offset);

        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], Crc32C.Compute(buffer[4..offset]));
    }

    /// <summary>Decodes and CRC-validates a metadata payload.</summary>
    /// <param name="buffer">The encoded metadata bytes.</param>
    /// <param name="meta">The decoded metadata when the payload is valid.</param>
    /// <returns><see langword="true" /> when the payload is structurally valid and its checksum matches.</returns>
    internal static bool TryDecodeMeta(ReadOnlySpan<byte> buffer, out GroupLogMetadata meta)
    {
        meta = default;

        // Fixed header plus three minimum length prefixes (groupId, fingerprint, votedFor).
        if (buffer.Length < MetaFixedByteCount + 12)
            return false;

        // A foreign magic or a version this node cannot decode is rejected outright.
        if (BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]) != MetaMagic)
            return false;

        if (buffer[4] != MetaVersion)
            return false;

        // The stored checksum covers everything after the magic, so any tampered or torn
        // metadata payload is caught before its fields are interpreted.
        var bodyLength = buffer.Length - 4 - 1 - 4;
        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buffer[^4..]);
        if (Crc32C.Compute(buffer.Slice(4, bodyLength + 1)) != storedCrc)
            return false;

        // Offset walks the fixed-width term/commit fields first, then the
        // length-prefixed variable fields that close the payload.
        var offset = 5;
        if (!TryReadFixedFields(buffer, ref offset, out var fields))
            return false;
        if (!TryReadString(buffer, ref offset, out var groupId))
            return false;
        if (!TryReadBytes(buffer, ref offset, out var fingerprint))
            return false;
        if (!TryReadString(buffer, ref offset, out var votedFor))
            return false;

        meta = new GroupLogMetadata(groupId, fingerprint, fields.ConfigurationGeneration, fields.CurrentTerm, votedFor, fields.LastLogIndex, fields.CommitIndex, fields.LastAppliedIndex);
        return true;
    }

    /// <summary>Computes the exact encoded length of a log frame.</summary>
    /// <param name="payloadLength">The entry payload length in bytes.</param>
    /// <returns>The encoded frame length in bytes.</returns>
    internal static int ComputeFrameEncodedLength(int payloadLength) => FrameHeaderByteCount + 4 + FrameFixedByteCount + payloadLength + 4;

    /// <summary>Encodes a single log frame (header + body + CRC) into a caller-provided buffer.</summary>
    /// <param name="buffer">The destination buffer of at least <see cref="ComputeFrameEncodedLength" /> bytes.</param>
    /// <param name="entry">The entry to encode.</param>
    internal static void EncodeFrame(Span<byte> buffer, FollowerLogEntry entry)
    {
        var payload = entry.PayloadSpan;
        var bodyLength = FrameFixedByteCount + payload.Length;
        var offset = 0;
        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], FrameMagic);
        offset += 4;
        buffer[offset] = FrameVersion;
        offset++;
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], bodyLength);
        offset += 4;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], entry.LogIndex);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], entry.Term);
        offset += 8;
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], payload.Length);
        offset += 4;
        payload.CopyTo(buffer[offset..]);
        offset += payload.Length;

        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], Crc32C.Compute(buffer[4..offset]));
    }

    /// <summary>Reads a single log frame from a buffer starting at a frame boundary.</summary>
    /// <param name="buffer">The buffer containing the frame.</param>
    /// <param name="entry">The decoded entry when the frame is valid.</param>
    /// <param name="consumed">The number of bytes consumed by the frame when valid.</param>
    /// <returns><see langword="true" /> when a complete, CRC-valid frame was read; otherwise <see langword="false" />.</returns>
    internal static bool TryReadFrame(ReadOnlySpan<byte> buffer, out FollowerLogEntry entry, out int consumed)
    {
        entry = default;
        consumed = 0;
        if (buffer.Length < FrameHeaderByteCount + 4)
            return false;

        if (BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]) != FrameMagic)
            return false;

        if (buffer[4] != FrameVersion)
            return false;

        var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(5, 4));
        if (bodyLength < FrameFixedByteCount || bodyLength > buffer.Length - 4 - 1 - 4 - 4)
            return false;

        const int bodyStart = FrameHeaderByteCount + 4;
        var crcOffset = bodyStart + bodyLength;
        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(crcOffset, 4));
        if (Crc32C.Compute(buffer[4..crcOffset]) != storedCrc)
            return false;

        var offset = bodyStart;
        var logIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var term = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var payloadLength = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
        offset += 4;
        if (payloadLength < 0 || payloadLength != bodyLength - FrameFixedByteCount)
            return false;

        var payload = OwnedBufferKit.CopyToOwned(buffer.Slice(offset, payloadLength));
        entry = new FollowerLogEntry(logIndex, term, payload);
        consumed = crcOffset + 4;
        return true;
    }

    private static void WriteString(Span<byte> buffer, string value, ref int offset)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], byteCount);
        offset += 4;
        _ = Encoding.UTF8.GetBytes(value, buffer[offset..]);
        offset += byteCount;
    }

    private static void WriteBytes(Span<byte> buffer, ReadOnlySpan<byte> value, ref int offset)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], value.Length);
        offset += 4;
        value.CopyTo(buffer[offset..]);
        offset += value.Length;
    }

    private static bool TryReadString(ReadOnlySpan<byte> buffer, ref int offset, out string value)
    {
        value = string.Empty;
        if (buffer.Length - offset < 4)
            return false;

        var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
        offset += 4;
        if (length < 0 || length > buffer.Length - offset)
            return false;

        value = Encoding.UTF8.GetString(buffer.Slice(offset, length));
        offset += length;
        return true;
    }

    private static bool TryReadBytes(ReadOnlySpan<byte> buffer, ref int offset, out ReadOnlyMemory<byte> value)
    {
        value = default;
        if (buffer.Length - offset < 4)
            return false;

        var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
        offset += 4;
        if (length < 0 || length > buffer.Length - offset)
            return false;

        value = OwnedBufferKit.CopyToOwned(buffer.Slice(offset, length));
        offset += length;
        return true;
    }

    private static bool TryReadFixedFields(ReadOnlySpan<byte> buffer, ref int offset, out MetaFixedFields fields)
    {
        fields = default;
        if (buffer.Length - offset < 8 * 5)
            return false;

        var generation = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var term = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var lastLogIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var commitIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var lastAppliedIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;

        fields = new MetaFixedFields(generation, term, lastLogIndex, commitIndex, lastAppliedIndex);
        return true;
    }

    /// <summary>The five fixed-width fields that open a metadata payload.</summary>
    /// <param name="ConfigurationGeneration">The configuration generation of the group.</param>
    /// <param name="CurrentTerm">The highest term this node has observed.</param>
    /// <param name="LastLogIndex">The durable last log index.</param>
    /// <param name="CommitIndex">The durable commit index.</param>
    /// <param name="LastAppliedIndex">The index last applied to memory by the coordinator.</param>
    [StructLayout(LayoutKind.Auto)]
    private readonly record struct MetaFixedFields(
        ulong ConfigurationGeneration,
        ulong CurrentTerm,
        ulong LastLogIndex,
        ulong CommitIndex,
        ulong LastAppliedIndex);

    /// <summary>Exact-size owned byte buffer helpers for replica-group encoding.</summary>
    private static class OwnedBufferKit
    {
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the caller retains ownership.
        internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
        {
            var owned = new byte[source.Length];
            source.CopyTo(owned);
            return owned;
        }
#pragma warning restore ZA0302
    }
}
