using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Entries;
using Squirix.Server.Storage.Journaling.Framing;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Testing;

namespace Squirix.Server.UnitTests.Support;

/// <summary>Writes binary journal segments for persistence unit tests.</summary>
internal static class BinaryJournalTestSegmentWriter
{
    public static Task<JournalRecord> BuildPutRecordAsync(ulong seq, string key, string value)
    {
        var body = JournalEntryPayload.Encode(new CacheEntry<object?> { Value = value, Version = 1 });
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

    public static Task WriteJournalSegmentAsync(string dir, int index, IReadOnlyList<JournalRecord> records)
    {
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}{index.ToString("000000", CultureInfo.InvariantCulture)}{StorageFileExtensions.Journal}");
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

    private static void WriteRecordFrame(Stream stream, JournalRecord record)
    {
        var encode = BinaryJournalCodec.PrepareEncode(record);
        BufferKit.WithBuffer(
            encode.BodyLength,
            (stream, record, encode),
            static (ctx, body) =>
            {
                _ = BinaryJournalCodec.Encode(ctx.record, body, in ctx.encode);
                JournalFraming.WriteFrame(ctx.stream, body);
            });
    }
}
