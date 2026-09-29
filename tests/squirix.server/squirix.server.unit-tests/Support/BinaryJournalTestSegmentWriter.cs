using System;
using System.Buffers.Binary;
using System.IO;
using System.Text;
using Microsoft.Win32.SafeHandles;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Writes binary journal segments for persistence unit tests.</summary>
internal static class BinaryJournalTestSegmentWriter
{
    internal static JournalRecord BuildBrokenPutRecord(ulong seq, string key)
    {
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = new byte[] { 1, 2, 3 },
        };
    }

    internal static JournalRecord BuildIdempotencyRecord(string operationId, string fingerprint, byte[] responseBytes, long unixMs, ulong seq)
    {
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = unixMs,
            Operation = JournalOperationKind.IdempotencyOutcome,
            Key = CacheKey.Default(operationId),
            IdempotencyOperationId = operationId,
            IdempotencyFingerprint = fingerprint,
            IdempotencyResponseBytes = responseBytes,
        };
    }

    internal static JournalRecord BuildPutRecord(ulong seq, string key, string value)
    {
        var body = JournalEntryPayloadKit.EncodePut(value);
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = body,
        };
    }

    internal static JournalRecord BuildPutRecord(ulong seq, string key, NodeCacheEntry<object?> entry)
    {
        var body = JournalEntryPayloadKit.Encode(entry);
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = body,
        };
    }

    internal static JournalRecord BuildRemoveRecord(ulong seq, string key)
    {
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.Remove,
            Key = CacheKey.Default(key),
        };
    }

    /// <summary>Writes a segment whose only frame carries the given raw opcode byte, framed with a valid checksum.</summary>
    /// <param name="dir">The data directory.</param>
    /// <param name="index">The segment index.</param>
    /// <param name="opcodeWire">The raw opcode byte of the frame.</param>
    /// <param name="key">The cache key of the frame.</param>
    /// <param name="followedBy">Valid records written after the raw frame in the same segment.</param>
    internal static void WriteRawOpcodeSegment(string dir, int index, byte opcodeWire, string key, ReadOnlySpan<JournalRecord> followedBy = default)
    {
        const int fixedPrefixSize = BinaryJournalCodec.FixedPrefixSize;
        var nsBytes = Encoding.UTF8.GetBytes(CacheKey.Default(key).Namespace);
        var keyBytes = Encoding.UTF8.GetBytes(key);
        var body = new byte[fixedPrefixSize + nsBytes.Length + keyBytes.Length];
        BinaryPrimitives.WriteUInt64LittleEndian(body, 1UL);
        BinaryPrimitives.WriteInt64LittleEndian(body.AsSpan(8), 1L);
        body[16] = opcodeWire;
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(17), ushort.CreateTruncating(nsBytes.Length));
        BinaryPrimitives.WriteUInt16LittleEndian(body.AsSpan(19), ushort.CreateTruncating(keyBytes.Length));
        nsBytes.CopyTo(body.AsSpan(fixedPrefixSize));
        keyBytes.CopyTo(body.AsSpan(fixedPrefixSize + nsBytes.Length));

        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        long offset = 0;
        WriteFileHeader(handle, ref offset);
        var frame = new byte[JournalFraming.FrameTotalLength(body.Length)];
        JournalFraming.WriteFrame(frame, body);
        RandomAccess.Write(handle, frame, offset);
        offset += frame.Length;
        for (var i = 0; i < followedBy.Length; i++)
            WriteRecordFrame(handle, ref offset, followedBy[i]);
    }

    internal static void WriteJournalSegment(string dir, int index, JournalRecord record)
    {
        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        WriteSegment(path, record);
    }

    internal static void WriteJournalSegment(string dir, int index, ReadOnlySpan<JournalRecord> records)
    {
        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        WriteSegment(path, records);
    }

    internal static void WriteSegment(string path, JournalRecord record)
    {
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        long offset = 0;
        WriteFileHeader(handle, ref offset);
        WriteRecordFrame(handle, ref offset, record);
    }

    internal static void WriteSegment(string path, ReadOnlySpan<JournalRecord> records)
    {
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        long offset = 0;
        WriteFileHeader(handle, ref offset);
        for (var i = 0; i < records.Length; i++)
            WriteRecordFrame(handle, ref offset, records[i]);
    }

    private static void WriteFileHeader(SafeFileHandle handle, ref long offset)
    {
        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        RandomAccess.Write(handle, header, offset);
        offset += header.Length;
    }

    private static void WriteRecordFrame(SafeFileHandle handle, ref long offset, JournalRecord record)
    {
        var encode = BinaryJournalCodec.PrepareEncode(record);
        var frameLength = JournalFraming.FrameTotalLength(encode.BodyLength);
        BufferKit.WithBuffer(
            frameLength,
            (Handle: handle, Record: record, Encode: encode, Offset: offset),
            static (ctx, frame) =>
            {
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                var body = frame.Slice(bodyOffset, ctx.Encode.BodyLength);
                _ = BinaryJournalCodec.Encode(ctx.Record, body, in ctx.Encode);
                JournalFraming.WriteFrame(frame, body);
                RandomAccess.Write(ctx.Handle, frame, ctx.Offset);
            });
        offset += frameLength;
    }
}
