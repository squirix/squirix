using System;
using System.IO;
using BenchmarkDotNet.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Codec;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;

namespace Squirix.Server.Benchmarks;

/// <summary>Cost of the startup scan that finds the next journal sequence over a journal of small put records.</summary>
[MemoryDiagnoser]
[ShortRunJob]
public class JournalRecoveryScanBenchmarks
{
    private const int FrameCount = 100_000;
    private const int MaxSegmentMb = 16;
    private const int PutEntrySize = 32;
    private const int SegmentIndex = 1;
    private string _dataDir = string.Empty;
    private State _manifest = new();
    private PersistenceOptions _options = new();

    /// <summary>Writes the journal segment with the codec and framing the production writer uses.</summary>
    [GlobalSetup]
    public void Setup()
    {
        _dataDir = Path.Join(Path.GetTempPath(), "squirix-recovery-scan-" + Guid.NewGuid().ToString("N"));
        _ = Directory.CreateDirectory(_dataDir);
        _options = new PersistenceOptions { DataDir = _dataDir, JournalMaxSegmentMb = MaxSegmentMb };
        _manifest = new State { CurrentJournal = SegmentIndex, NextSequence = 1 };

        var record = new JournalRecord
        {
            Sequence = 1,
            UnixMs = 1,
            Operation = JournalOperationKind.Put,
            Key = new CacheKey("bench", "scan-key"),
            PutEntryBytes = new byte[PutEntrySize],
        };
        var encode = BinaryJournalCodec.PrepareEncode(record);
        var frameLength = JournalFraming.FrameTotalLength(encode.BodyLength);
        var bytes = new byte[JournalFraming.FileHeaderSize + (frameLength * FrameCount)];
        JournalFraming.WriteFileHeader(bytes.AsSpan(0, JournalFraming.FileHeaderSize));
        for (var i = 0; i < FrameCount; i++)
        {
            record.Sequence = ulong.CreateChecked(i) + 1UL;
            var frame = bytes.AsSpan(JournalFraming.FileHeaderSize + (i * frameLength), frameLength);
            _ = BinaryJournalCodec.Encode(record, frame.Slice(JournalFraming.FrameHeaderSize, encode.BodyLength), in encode);
            JournalFraming.SealFrame(frame);
        }

        File.WriteAllBytes(JournalReadPath.BuildSegmentPath(_dataDir, SegmentIndex), bytes);
    }

    /// <summary>Deletes the scratch directory.</summary>
    [GlobalCleanup]
    public void Cleanup() => Directory.Delete(_dataDir, true);

    /// <summary>Scans the whole journal for the next sequence.</summary>
    /// <returns>The next journal sequence.</returns>
    [Benchmark]
    public ulong DetermineNextSequence() => JournalRecoveryScan.DetermineNextSequence(_manifest, _options);
}
