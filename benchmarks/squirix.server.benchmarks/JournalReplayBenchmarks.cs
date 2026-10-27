using System;
using System.Globalization;
using System.IO;
using System.Threading;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of replaying a journal segment of small put frames through the replay enumerator.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class JournalReplayBenchmarks
{
    private const int EntrySize = 64;
    private const int BatchBytes = 1024 * 1024;
    private const int FrameCount = 100_000;
    private const int SegmentIndex = 1;
    private string _dataDir = string.Empty;

    /// <summary>Removes the journal directory.</summary>
    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dataDir, true);

    /// <summary>Replays every record of the segment.</summary>
    /// <returns>The number of replayed records.</returns>
    [Benchmark(OperationsPerInvoke = FrameCount)]
    public int ReplayAll()
    {
        var count = 0;
        using var records = JournalReadPath.ReadAll(_dataDir, SegmentIndex, CancellationToken.None);
        while (records.MoveNext())
            count++;

        return count;
    }

    /// <summary>Writes a segment of <see cref="FrameCount" /> put frames with the real codec and writer.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _dataDir = Path.Join(Path.GetTempPath(), "journal-replay-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(_dataDir);
        using var writer = JournalSegmentWriterFactory.Create();
        writer.OpenSegment(JournalReadPath.BuildSegmentPath(_dataDir, SegmentIndex), false);

        Span<byte> header = stackalloc byte[JournalFraming.FileHeaderSize];
        JournalFraming.WriteFileHeader(header);
        writer.Write(header, 0);

        long offset = header.Length;
        var batch = new byte[BatchBytes];
        var used = 0;
        for (ulong i = 1; i <= FrameCount; i++)
        {
            var record = new JournalRecord
            {
                Sequence = i,
                UnixMs = 1,
                Operation = JournalOperationKind.Put,
                Key = new CacheKey("bench", "key-" + i.ToString(CultureInfo.InvariantCulture)),
                PutEntryBytes = new byte[EntrySize],
            };
            var prepared = BinaryJournalCodec.PrepareEncode(record);
            var frameLength = JournalFraming.FrameTotalLength(prepared.BodyLength);
            if (used + frameLength > batch.Length)
            {
                writer.Write(batch.AsSpan(0, used), offset);
                offset += used;
                used = 0;
            }

            var frame = batch.AsSpan(used, frameLength);
            _ = BinaryJournalCodec.Encode(record, frame[JournalFraming.FrameHeaderSize..^JournalFrameEnvelope.FooterSize], in prepared);
            JournalFraming.SealFrame(frame);
            used += frameLength;
        }

        writer.Write(batch.AsSpan(0, used), offset);
        writer.FlushToDisk();
    }
}
