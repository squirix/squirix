using System;
using System.Buffers.Binary;
using System.IO;
using System.Runtime.InteropServices;
using System.Text;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Storage.Replication;

/// <summary>Binary codec for replica-group metadata and log frames with CRC32C integrity.</summary>
/// <remarks>
///     <para>On-disk layout is little-endian throughout.</para>
///     <para>
///     Metadata record: <c language="csharp">magic(4) | version(1) | payload | crc32c(payload)(4)</c> where payload carries the
///     fixed-width term/commit fields followed by length-prefixed UTF-8 strings and the topology fingerprint. The record is
///     stored in a slot of <see cref="GroupMetadataSlots" />.
///     </para>
///     <para>
///     Log frame: <c language="csharp">bodyLength(4) | body | crc32c(body)(4)</c> where body carries
///     <c language="csharp">logIndex(8) | term(8) | payloadLength(4) | payload</c>. Frames are append-only; a torn tail frame is
///     detected by truncation or CRC mismatch.
///     </para>
/// </remarks>
internal static class GroupLogCodec
{
    /// <summary>Frame preamble byte count: magic(4) + version(1). The full frame header adds the 4-byte body length.</summary>
    internal const int FramePreambleByteCount = sizeof(uint) + sizeof(byte);

    /// <summary>Fixed body byte count: logIndex(8) + term(8) + payloadLength(4).</summary>
    private const int FrameFixedByteCount = sizeof(ulong) + sizeof(ulong) + sizeof(int);

    /// <summary>Byte size of the body-length field opening every frame.</summary>
    private const int FrameLengthFieldSize = sizeof(int);

    /// <summary>Byte size of the CRC32C checksum closing every frame and metadata file.</summary>
    private const int CrcByteCount = sizeof(uint);

    /// <summary>Byte size of the length prefix framing every variable-length metadata field.</summary>
    private const int MetaLengthPrefixSize = sizeof(int);

    /// <summary>Log frame magic bytes, <c language="csharp">"SQRL"</c>.</summary>
    private const uint FrameMagic = 0x4C525153u;

    /// <summary>Log format version.</summary>
    private const byte FrameVersion = 1;

    /// <summary>Maximum accepted log frame body length; mirrors the journal frame payload cap so a corrupt header cannot force a multi-GB pool rent.</summary>
    private const int MaxFrameBodyLength = FrameFixedByteCount + (512 * 1024 * 1024);

    /// <summary>Byte size of the fixed-width fields of a metadata payload: four ulongs + member state(1) + bootstrap offered(1).</summary>
    private const int MetaFieldsByteCount = (sizeof(ulong) * 4) + sizeof(byte) + sizeof(byte);

    /// <summary>Fixed metadata size: magic(4) + version(1) + the fixed-width fields.</summary>
    private const int MetaFixedByteCount = sizeof(uint) + sizeof(byte) + MetaFieldsByteCount;

    /// <summary>Metadata file magic bytes, <c language="csharp">"SQRM"</c>.</summary>
    private const uint MetaMagic = 0x4D525153u;

    /// <summary>Stored byte of <see cref="GroupMemberState.Unseeded" />.</summary>
    private const byte UnseededByte = 0;

    /// <summary>Stored byte of <see cref="GroupMemberState.Complete" />.</summary>
    private const byte CompleteByte = 1;

    /// <summary>Stored byte of <see cref="GroupMemberState.Incomplete" />.</summary>
    private const byte IncompleteByte = 2;

    /// <summary>Stored byte of a bootstrap offer that was not made.</summary>
    private const byte NotOfferedByte = 0;

    /// <summary>Stored byte of a bootstrap offer that was made.</summary>
    private const byte OfferedByte = 1;

    /// <summary>Metadata format version.</summary>
    private const byte MetaVersion = 2;

    /// <summary>Computes the exact encoded length of a log frame.</summary>
    /// <param name="len">The entry payload length in bytes.</param>
    /// <returns>The encoded frame length in bytes.</returns>
    /// <exception cref="InvalidDataException">Thrown when the entry payload exceeds the maximum allowed frame body length.</exception>
    internal static int ComputeFrameEncodedLength(int len)
    {
        const string message = "Log frame payload exceeds the maximum allowed length and cannot be restored on read.";
        return len > MaxFrameBodyLength - FrameFixedByteCount
            ? throw new InvalidDataException(message)
            : FramePreambleByteCount + FrameLengthFieldSize + FrameFixedByteCount + len + CrcByteCount;
    }

    /// <summary>Computes the encoded length of a metadata payload.</summary>
    /// <param name="meta">The metadata to encode.</param>
    /// <returns>The encoded metadata length in bytes.</returns>
    internal static int ComputeMetaEncodedLength(in GroupLogMetadata meta)
    {
        ArgumentNullException.ThrowIfNull(meta.GroupId);
        var groupBytes = Encoding.UTF8.GetByteCount(meta.GroupId);
        var votedBytes = Encoding.UTF8.GetByteCount(meta.VotedFor);
        return MetaFixedByteCount + MetaLengthPrefixSize + groupBytes + MetaLengthPrefixSize + meta.TopologyFingerprint.Length + MetaLengthPrefixSize + votedBytes + CrcByteCount;
    }

    /// <summary>Encodes a single log frame (header + body + CRC) into a caller-provided buffer.</summary>
    /// <param name="buffer">The destination buffer of at least <see cref="ComputeFrameEncodedLength" /> bytes.</param>
    /// <param name="entry">The entry to encode.</param>
    /// <exception cref="InvalidDataException">Thrown when the entry payload exceeds the maximum allowed frame body length.</exception>
    internal static void EncodeFrame(Span<byte> buffer, in FollowerLogEntry entry)
    {
        var payload = entry.PayloadSpan;
        if (payload.Length > MaxFrameBodyLength - FrameFixedByteCount)
            throw new InvalidDataException("Log frame payload exceeds the maximum allowed length and cannot be restored on read.");

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

    /// <summary>Encodes metadata into a caller-provided buffer of at least <see cref="ComputeMetaEncodedLength" /> bytes.</summary>
    /// <param name="meta">The metadata to encode.</param>
    /// <param name="buffer">The destination buffer.</param>
    internal static void EncodeMeta(in GroupLogMetadata meta, Span<byte> buffer)
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
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.CommitIndex);
        offset += 8;
        BinaryPrimitives.WriteUInt64LittleEndian(buffer[offset..], meta.LastAppliedIndex);
        offset += 8;
        buffer[offset] = ToStateByte(meta.MemberState);
        offset++;
        buffer[offset] = meta.BootstrapOffered ? OfferedByte : NotOfferedByte;
        offset++;

        WriteString(buffer, meta.GroupId, ref offset);
        WriteBytes(buffer, meta.TopologyFingerprint.Span, ref offset);
        WriteString(buffer, meta.VotedFor, ref offset);

        BinaryPrimitives.WriteUInt32LittleEndian(buffer[offset..], Crc32C.Compute(buffer[4..offset]));
    }

    /// <summary>Checks whether a metadata payload carries a metadata version below the current one.</summary>
    /// <param name="buffer">The encoded metadata bytes.</param>
    /// <returns><see langword="true" /> when the payload opens with the metadata magic and an earlier version byte.</returns>
    internal static bool IsEarlierMetaVersion(ReadOnlySpan<byte> buffer) =>
        buffer.Length > sizeof(uint) && BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]) == MetaMagic && buffer[4] < MetaVersion;

    /// <summary>Decodes and CRC-validates a metadata payload.</summary>
    /// <param name="buffer">The encoded metadata bytes.</param>
    /// <param name="meta">The decoded metadata when the payload is valid.</param>
    /// <returns><see langword="true" /> when the payload is structurally valid and its checksum matches.</returns>
    internal static bool TryDecodeMeta(ReadOnlySpan<byte> buffer, out GroupLogMetadata meta)
    {
        meta = default;

        // Fixed header plus three minimum length prefixes (groupId, fingerprint, votedFor).
        if (buffer.Length < MetaFixedByteCount + (MetaLengthPrefixSize * 3))
            return false;

        // A foreign magic or a version this node cannot decode is rejected outright.
        if (BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]) != MetaMagic)
            return false;

        if (buffer[4] != MetaVersion)
            return false;

        // The stored checksum covers everything after the magic, so any tampered or torn
        // metadata payload is caught before its fields are interpreted.
        var bodyLength = buffer.Length - sizeof(uint) - sizeof(byte) - CrcByteCount;
        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buffer[^CrcByteCount..]);

        // Offset walks the fixed-width term/commit fields first, then the
        // length-prefixed variable fields that close the payload.
        return Crc32C.Compute(buffer.Slice(4, bodyLength + 1)) == storedCrc && TryReadMetaFields(buffer, out meta);
    }

    /// <summary>Reads a single log frame from a buffer starting at a frame boundary.</summary>
    /// <param name="buffer">The buffer containing the frame.</param>
    /// <param name="entry">The decoded entry when the frame is valid.</param>
    /// <returns><see langword="true" /> when a complete, CRC-valid frame was read; otherwise <see langword="false" />.</returns>
    internal static bool TryReadFrame(ReadOnlySpan<byte> buffer, out FollowerLogEntry entry)
    {
        entry = default;
        if (!TryReadFrameCore(buffer, out var logIndex, out var term, out var payloadStart, out var payloadLength))
            return false;

        entry = new FollowerLogEntry(logIndex, term, BufferEx.CopyToOwned(buffer.Slice(payloadStart, payloadLength)));
        return true;
    }

    /// <summary>Validates a complete log frame and reads its index and term without allocating the payload.</summary>
    /// <param name="buffer">The buffer containing the frame.</param>
    /// <param name="logIndex">The decoded entry index when the frame is valid.</param>
    /// <param name="term">The decoded entry term when the frame is valid.</param>
    /// <returns><see langword="true" /> when a complete, CRC-valid frame was read; otherwise <see langword="false" />.</returns>
    internal static bool TryReadFrameFields(ReadOnlySpan<byte> buffer, out ulong logIndex, out ulong term)
    {
        logIndex = 0;
        term = 0;
        return TryReadFrameCore(buffer, out logIndex, out term, out _, out _);
    }

    /// <summary>Reads the total encoded length of a log frame from its 9-byte header (magic, version, body length).</summary>
    /// <param name="header">The 9-byte frame header: magic(4), version(1), bodyLength(4).</param>
    /// <param name="frameLength">The total frame length including the header, body and CRC when valid.</param>
    /// <returns><see langword="true" /> when the header is structurally valid; otherwise <see langword="false" />.</returns>
    internal static bool TryReadFrameHeaderLength(ReadOnlySpan<byte> header, out int frameLength)
    {
        frameLength = 0;
        if (header.Length < FramePreambleByteCount + FrameLengthFieldSize)
            return false;

        if (BinaryPrimitives.ReadUInt32LittleEndian(header[..4]) != FrameMagic)
            return false;

        if (header[4] != FrameVersion)
            return false;

        var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(5, 4));
        if (bodyLength is not (>= FrameFixedByteCount and <= MaxFrameBodyLength))
            return false;

        frameLength = FramePreambleByteCount + FrameLengthFieldSize + bodyLength + CrcByteCount;
        return true;
    }

    private static bool IsFrameBodyLengthWithinBounds(int length, int bufferLength, int bodyStart) =>
        length is >= FrameFixedByteCount and <= MaxFrameBodyLength && length <= bufferLength - bodyStart - CrcByteCount;

    private static byte ToStateByte(GroupMemberState state) => state switch
    {
        GroupMemberState.Unseeded => UnseededByte,
        GroupMemberState.Complete => CompleteByte,
        GroupMemberState.Incomplete => IncompleteByte,
        _ => throw new ArgumentOutOfRangeException(nameof(state), state, "Unsupported group member state."),
    };

    private static bool TryReadBytes(ReadOnlySpan<byte> buffer, ref int offset, out ReadOnlyMemory<byte> value)
    {
        value = default;
        if (buffer.Length - offset < MetaLengthPrefixSize)
            return false;

        var length = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
        offset += 4;
        if (length < 0 || length > buffer.Length - offset)
            return false;

        value = BufferEx.CopyToOwned(buffer.Slice(offset, length));
        offset += length;
        return true;
    }

    private static bool TryReadFixedFields(ReadOnlySpan<byte> buffer, ref int offset, out MetaFixedFields fields)
    {
        fields = default;
        if (buffer.Length - offset < MetaFieldsByteCount)
            return false;

        var generation = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var term = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var commitIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var lastAppliedIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        var stateByte = buffer[offset];
        offset++;
        var offered = buffer[offset];
        offset++;

        // A state or offer byte outside the written values is damage, never a value to interpret.
        if (!TryReadMemberState(stateByte, out var state) || offered > OfferedByte)
            return false;

        fields = new MetaFixedFields(generation, term, commitIndex, lastAppliedIndex, state, offered == OfferedByte);
        return true;
    }

    /// <summary>Validates a CRC-complete log frame and reads its index, term and payload extent without copying the payload.</summary>
    /// <param name="buffer">The buffer containing the frame.</param>
    /// <param name="logIndex">The decoded entry index when the frame is valid.</param>
    /// <param name="term">The decoded entry term when the frame is valid.</param>
    /// <param name="payloadStart">The payload start offset within the buffer when the frame is valid.</param>
    /// <param name="payloadLength">The payload length in bytes when the frame is valid.</param>
    /// <returns><see langword="true" /> when a complete, CRC-valid frame was read; otherwise <see langword="false" />.</returns>
    private static bool TryReadFrameCore(ReadOnlySpan<byte> buffer, out ulong logIndex, out ulong term, out int payloadStart, out int payloadLength)
    {
        logIndex = 0;
        term = 0;
        payloadStart = 0;
        payloadLength = 0;

        // A frame is header(5) + length(4) + body + crc(4); anything shorter cannot be validated.
        if (buffer.Length < FramePreambleByteCount + FrameLengthFieldSize)
            return false;

        // The magic and version guard against reading a metadata file or an unrelated format.
        if (BinaryPrimitives.ReadUInt32LittleEndian(buffer[..4]) != FrameMagic)
            return false;

        if (buffer[4] != FrameVersion)
            return false;

        const int bodyStart = FramePreambleByteCount + FrameLengthFieldSize;

        // The declared body must hold the fixed fields, stay within the maximum bound, and leave room for the trailing crc inside the buffer.
        var bodyLength = BinaryPrimitives.ReadInt32LittleEndian(buffer.Slice(5, 4));
        if (!IsFrameBodyLengthWithinBounds(bodyLength, buffer.Length, bodyStart))
            return false;

        var crcOffset = bodyStart + bodyLength;
        var storedCrc = BinaryPrimitives.ReadUInt32LittleEndian(buffer.Slice(crcOffset, 4));

        // The crc covers everything between the header and the trailing checksum; a mismatch marks a torn or corrupt frame.
        if (Crc32C.Compute(buffer[4..crcOffset]) != storedCrc)
            return false;

        var offset = bodyStart;
        logIndex = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        term = BinaryPrimitives.ReadUInt64LittleEndian(buffer[offset..]);
        offset += 8;
        payloadLength = BinaryPrimitives.ReadInt32LittleEndian(buffer[offset..]);
        offset += 4;

        // The payload must be non-negative and exactly consume the declared body after the fixed fields.
        if (payloadLength < 0 || payloadLength != bodyLength - FrameFixedByteCount)
            return false;

        payloadStart = offset;
        return true;
    }

    /// <summary>Reads the member state byte.</summary>
    /// <param name="value">The encoded byte.</param>
    /// <param name="state">The decoded state; <see cref="GroupMemberState.Unseeded" /> when the byte is not a written value.</param>
    /// <returns><see langword="true" /> when the byte is one of the written values; the caller must not use <paramref name="state" /> otherwise.</returns>
    private static bool TryReadMemberState(byte value, out GroupMemberState state)
    {
        state = value switch
        {
            CompleteByte => GroupMemberState.Complete,
            IncompleteByte => GroupMemberState.Incomplete,
            _ => GroupMemberState.Unseeded,
        };
        return value <= IncompleteByte;
    }

    /// <summary>Reads the term/commit fixed fields and the trailing variable fields into a metadata value.</summary>
    /// <param name="buffer">The CRC-validated encoded metadata payload.</param>
    /// <param name="meta">The decoded metadata when every field is structurally valid.</param>
    /// <returns><see langword="true" /> when all fields decode; otherwise <see langword="false" />.</returns>
    private static bool TryReadMetaFields(ReadOnlySpan<byte> buffer, out GroupLogMetadata meta)
    {
        meta = default;
        var offset = 5;
        if (!TryReadFixedFields(buffer, ref offset, out var fields))
            return false;
        if (!TryReadString(buffer, ref offset, out var groupId))
            return false;
        if (!TryReadBytes(buffer, ref offset, out var fingerprint))
            return false;
        if (!TryReadString(buffer, ref offset, out var votedFor))
            return false;

        meta = new GroupLogMetadata(
            groupId,
            fingerprint,
            fields.ConfigurationGeneration,
            fields.CurrentTerm,
            votedFor,
            fields.CommitIndex,
            fields.LastAppliedIndex,
            fields.MemberState,
            fields.BootstrapOffered);
        return true;
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

    private static void WriteBytes(Span<byte> buffer, ReadOnlySpan<byte> value, ref int offset)
    {
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], value.Length);
        offset += 4;
        value.CopyTo(buffer[offset..]);
        offset += value.Length;
    }

    private static void WriteString(Span<byte> buffer, string value, ref int offset)
    {
        var byteCount = Encoding.UTF8.GetByteCount(value);
        BinaryPrimitives.WriteInt32LittleEndian(buffer[offset..], byteCount);
        offset += 4;
        _ = Encoding.UTF8.GetBytes(value, buffer[offset..]);
        offset += byteCount;
    }

    /// <summary>The fixed-width fields that open a metadata payload.</summary>
    /// <param name="ConfigurationGeneration">The configuration generation of the group.</param>
    /// <param name="CurrentTerm">The highest term this node has observed.</param>
    /// <param name="CommitIndex">The durable commit index.</param>
    /// <param name="LastAppliedIndex">The index last applied to memory by the coordinator.</param>
    /// <param name="MemberState">Whether this copy holds the data of the group.</param>
    /// <param name="BootstrapOffered">Whether the owner of the group offered this copy a place in the first start of the group.</param>
    [StructLayout(LayoutKind.Auto)]
    [Immutable]
    private readonly record struct MetaFixedFields(
        ulong ConfigurationGeneration,
        ulong CurrentTerm,
        ulong CommitIndex,
        ulong LastAppliedIndex,
        GroupMemberState MemberState,
        bool BootstrapOffered);
}
