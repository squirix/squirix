using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Journaling;
using Squirix.Server.TestKit.Testing;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Writes binary journal segments for persistence unit tests.</summary>
internal static class BinaryJournalTestSegmentWriter
{
    public static Task WriteJournalSegmentAsync(string dir, int index, IReadOnlyList<JournalRecord> records)
    {
        var path = PathKit.Combine(dir, $"{FilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{FileExtensions.Journal}");
        return WriteSegmentAsync(path, records);
    }

    public static async Task WriteSegmentAsync(string path, IReadOnlyList<JournalRecord> records)
    {
        await using var stream = new FileStream(path, FileMode.Create, FileAccess.Write, FileShare.Read);
        JournalFraming.WriteFileHeader(stream);
        for (var i = 0; i < records.Count; i++)
            WriteRecordFrame(stream, records[i]);

        await stream.FlushAsync(CancellationToken.None);
    }

    internal static Task<JournalRecord> BuildPutRecordAsync(ulong seq, string key, string value)
    {
        var body = JournalEntryPayloadKit.EncodePut(value);
        return Task.FromResult(
            new JournalRecord
            {
                Sequence = seq,
                UnixMs = 1,
                Operation = JournalOperationKind.Put,
                Key = CacheKey.Default(key),
                PutEntryBytes = body,
            });
    }

    private static void WriteRecordFrame(Stream stream, JournalRecord record)
    {
        var encode = BinaryJournalCodec.PrepareEncode(record);
        var frameLength = JournalFraming.FrameTotalLength(encode.BodyLength);
        BufferKit.WithBuffer(
            frameLength,
            (stream, record, encode),
            static (ctx, frame) =>
            {
                const int bodyOffset = JournalFraming.FrameHeaderSize;
                var body = frame.Slice(bodyOffset, ctx.encode.BodyLength);
                _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.encode);
                JournalFraming.WriteFrame(frame, body);
                ctx.stream.Write(frame);
            });
    }
}
