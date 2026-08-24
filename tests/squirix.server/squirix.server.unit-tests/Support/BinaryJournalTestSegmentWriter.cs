using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
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
    internal static Task<JournalRecord> BuildPutRecordAsync(ulong seq, string key, string value)
    {
        var body = JournalEntryPayloadKit.EncodePut(value);
        var record = new JournalRecord
        {
            Sequence = seq,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = CacheKey.Default(key),
            PutEntryBytes = body,
        };
        return Task.FromResult(record);
    }

    internal static Task WriteJournalSegmentAsync(string dir, int index, JournalRecord record)
    {
        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        return WriteSegmentAsync(path, record);
    }

    internal static Task WriteJournalSegmentAsync(string dir, int index, IReadOnlyList<JournalRecord> records)
    {
        var path = NodePathKit.Combine(dir, $"{FilePrefixes.Journal}{NodeInvariantIndexStrings.FormatD6(index)}{FileExtensions.Journal}");
        return WriteSegmentAsync(path, records);
    }

    internal static async Task WriteSegmentAsync(string path, JournalRecord record)
    {
        using var handle = File.OpenHandle(path, FileMode.Create, FileAccess.Write);
        long offset = 0;
        WriteFileHeader(handle, ref offset);
        WriteRecordFrame(handle, ref offset, record);
    }

    internal static async Task WriteSegmentAsync(string path, IReadOnlyList<JournalRecord> records)
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
