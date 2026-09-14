using System;
using System.Collections.Generic;
using System.IO;
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

    internal static JournalRecord BuildRemoveExpirationRecord(ulong seq, string key)
    {
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.RemoveExpiration,
            Key = CacheKey.Default(key),
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

    internal static JournalRecord BuildTouchExpirationRecord(ulong seq, string key, DateTime expiresUtc)
    {
        return new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.TouchExpiration,
            Key = CacheKey.Default(key),
            TouchExpirationUtc = expiresUtc,
        };
    }

    internal static void WriteJournalSegment(string dir, int index, JournalRecord record)
    {
        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        WriteSegment(path, record);
    }

    internal static void WriteJournalSegment(string dir, int index, IReadOnlyList<JournalRecord> records)
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

    internal static void WriteSegment(string path, IReadOnlyList<JournalRecord> records)
    {
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        long offset = 0;
        WriteFileHeader(handle, ref offset);
        for (var i = 0; i < records.Count; i++)
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
