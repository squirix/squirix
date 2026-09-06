using System;
using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Text;
using Squirix.Server.Attributes;

namespace Squirix.Server.Cluster.Replication;

/// <summary>Canonical binary encoding of <see cref="ReplicaLogRecord" /> for durable log payloads.</summary>
/// <remarks>
/// The encoding is explicit field order with 32-bit little-endian length prefixes; decoding rejects
/// truncated buffers, trailing bytes, and unknown versions. The payload checksum rides opaquely inside
/// the encoding (journal frames add their own CRC); owner, follower log, and follower applier observe
/// identical bytes without sharing anything but this codec.
/// </remarks>
[Immutable]
internal static class ReplicaLogCodec
{
    private const ushort Version = 1;

    /// <summary>Encodes a record to its canonical bytes.</summary>
    /// <param name="record">The record to encode.</param>
    /// <returns>The canonical payload bytes.</returns>
    /// <exception cref="InvalidOperationException">Thrown when a field exceeds the 32-bit length prefix.</exception>
    [SuppressMessage("Usage", "MA0045:Use async disposable", Justification = "BinaryWriter and MemoryStream are in-memory and are intentionally encoded synchronously before durable asynchronous I/O.")]
    internal static byte[] Encode(in ReplicaLogRecord record)
    {
        // Project the exact payload length before serializing so a pathological record is rejected
        // before the buffer can grow.
        var length = 2L + 8 + 8;
        length += 4L + Encoding.UTF8.GetByteCount(record.OperationId) + 4L + Encoding.UTF8.GetByteCount(record.OperationScope);
        length += 4L + record.OperationFingerprint.Length;
        length += 4L + Encoding.UTF8.GetByteCount(record.RecordKind) + 4L + Encoding.UTF8.GetByteCount(record.CacheName);
        length += 4L + record.KeyPayload.Length;
        length += 4L + Encoding.UTF8.GetByteCount(record.MutationKind);
        length += 4L + record.MutationPayload.Length;
        length += 4L + record.OutcomePayload.Length;
        length += 8 + 8 + 8 + 4;
        if (length > int.MaxValue)
            throw new InvalidOperationException("Replica log record exceeds the maximum encoded length.");

        using var stream = new MemoryStream();
        using (var writer = new BinaryWriter(stream, Encoding.UTF8, true))
        {
            writer.Write(Version);
            writer.Write(record.LogIndex);
            writer.Write(record.Term);
            WriteString(writer, record.OperationId);
            WriteString(writer, record.OperationScope);
            WriteBytes(writer, record.OperationFingerprint.Span);
            WriteString(writer, record.RecordKind);
            WriteString(writer, record.CacheName);
            WriteBytes(writer, record.KeyPayload.Span);
            WriteString(writer, record.MutationKind);
            WriteBytes(writer, record.MutationPayload.Span);
            WriteBytes(writer, record.OutcomePayload.Span);
            writer.Write(record.ExpiresUtcTicks);
            writer.Write(record.CreatedUtcTicks);
            writer.Write(record.ResolvedUtcTicks);
            writer.Write(record.PayloadChecksum);
        }

        return stream.ToArray();
    }

    /// <summary>Decodes canonical bytes back to a record.</summary>
    /// <param name="bytes">The canonical payload bytes.</param>
    /// <returns>The decoded record, or <see langword="null" /> when the payload is invalid.</returns>
    internal static ReplicaLogRecord? Decode(ReadOnlyMemory<byte> bytes)
    {
        var span = bytes.Span;
        if (span.Length < 2 || BinaryPrimitives.ReadUInt16LittleEndian(span) != Version)
            return null;

        var decoder = new Decoder(bytes.Slice(2));
        if (decoder.ReadHead() is not { } head || decoder.ReadMiddle() is not { } middle ||
            decoder.ReadTail() is not { } tail || !decoder.AtEnd)
            return null;

        return new ReplicaLogRecord(
            head.LogIndex,
            head.Term,
            head.OperationId,
            head.OperationScope,
            head.OperationFingerprint,
            head.RecordKind,
            head.CacheName,
            middle.KeyPayload,
            middle.MutationKind,
            middle.MutationPayload,
            middle.OutcomePayload,
            tail.ExpiresUtcTicks,
            tail.CreatedUtcTicks,
            tail.ResolvedUtcTicks,
            tail.PayloadChecksum);
    }

    private static void WriteBytes(BinaryWriter writer, ReadOnlySpan<byte> value)
    {
        writer.Write(value.Length);
        writer.Write(value);
    }

    private static void WriteString(BinaryWriter writer, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        writer.Write(bytes.Length);
        writer.Write(bytes);
    }

    /// <summary>Exact-size owned byte buffer helper for decoder output.</summary>
    /// <remarks>
    /// The decoded record outlives the source buffer, so fields need owned copies that the span
    /// cannot lend. The escape is exact-size and caller-retained.
    /// </remarks>
    private static class OwnedBufferKit
    {
#pragma warning disable ZA0302 // ZA0302: exact-size owned buffer escape; the decoder output retains ownership.
        internal static byte[] CopyToOwned(ReadOnlySpan<byte> source)
        {
            var owned = new byte[source.Length];
            source.CopyTo(owned);
            return owned;
        }
#pragma warning restore ZA0302
    }

    /// <summary>Cursor reader for canonical record fields with bounds checks.</summary>
    private sealed class Decoder
    {
        private readonly ReadOnlyMemory<byte> _buffer;
        private int _offset;

        internal Decoder(ReadOnlyMemory<byte> buffer)
        {
            _buffer = buffer;
        }

        internal bool AtEnd => _offset == _buffer.Length;

        internal (ulong LogIndex, ulong Term, string OperationId, string OperationScope, ReadOnlyMemory<byte> OperationFingerprint, string RecordKind, string CacheName)? ReadHead()
        {
            if (!TryTake(8, out var logIndexBytes) || !TryTake(8, out var termBytes) ||
                !TryTakeLengthPrefixed(out var operationIdBytes) || !TryTakeLengthPrefixed(out var operationScopeBytes) ||
                !TryTakeLengthPrefixed(out var fingerprintBytes) || !TryTakeLengthPrefixed(out var recordKindBytes) ||
                !TryTakeLengthPrefixed(out var cacheNameBytes))
                return null;

            return (
                BinaryPrimitives.ReadUInt64LittleEndian(logIndexBytes),
                BinaryPrimitives.ReadUInt64LittleEndian(termBytes),
                Encoding.UTF8.GetString(operationIdBytes),
                Encoding.UTF8.GetString(operationScopeBytes),
                OwnedBufferKit.CopyToOwned(fingerprintBytes),
                Encoding.UTF8.GetString(recordKindBytes),
                Encoding.UTF8.GetString(cacheNameBytes));
        }

        internal (ReadOnlyMemory<byte> KeyPayload, string MutationKind, ReadOnlyMemory<byte> MutationPayload, ReadOnlyMemory<byte> OutcomePayload)? ReadMiddle()
        {
            if (!TryTakeLengthPrefixed(out var keyBytes) || !TryTakeLengthPrefixed(out var mutationKindBytes) ||
                !TryTakeLengthPrefixed(out var mutationBytes) || !TryTakeLengthPrefixed(out var outcomeBytes))
                return null;

            return (
                OwnedBufferKit.CopyToOwned(keyBytes),
                Encoding.UTF8.GetString(mutationKindBytes),
                OwnedBufferKit.CopyToOwned(mutationBytes),
                OwnedBufferKit.CopyToOwned(outcomeBytes));
        }

        internal (long ExpiresUtcTicks, long CreatedUtcTicks, long ResolvedUtcTicks, uint PayloadChecksum)? ReadTail()
        {
            if (!TryTake(8, out var expiresBytes) || !TryTake(8, out var createdBytes) ||
                !TryTake(8, out var resolvedBytes) || !TryTake(4, out var checksumBytes))
                return null;

            return (
                BinaryPrimitives.ReadInt64LittleEndian(expiresBytes),
                BinaryPrimitives.ReadInt64LittleEndian(createdBytes),
                BinaryPrimitives.ReadInt64LittleEndian(resolvedBytes),
                BinaryPrimitives.ReadUInt32LittleEndian(checksumBytes));
        }

        private bool TryTake(int size, out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (size < 0 || size > _buffer.Length - _offset)
                return false;

            slice = _buffer.Span.Slice(_offset, size);
            _offset += size;
            return true;
        }

        private bool TryTakeLengthPrefixed(out ReadOnlySpan<byte> slice)
        {
            slice = default;
            if (!TryTake(4, out var lengthBytes))
                return false;

            var length = BinaryPrimitives.ReadInt32LittleEndian(lengthBytes);
            return length >= 0 && TryTake(length, out slice);
        }
    }
}
