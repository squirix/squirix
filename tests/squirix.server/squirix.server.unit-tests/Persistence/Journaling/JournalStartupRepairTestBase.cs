using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.Storage.Manifest;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Shared fixtures for journal startup repair tests.</summary>
public abstract class JournalStartupRepairTestBase : IsolatedStorageTestBase
{
    private protected static byte[] BadHeader() => [0x42, 0x41, 0x44, 0x21, JournalFraming.Version];

    private protected static byte[] Concat(params byte[][] parts)
    {
        var total = 0;
        foreach (var part in parts)
            total += part.Length;

        var result = new byte[total];
        var offset = 0;
        foreach (var part in parts)
        {
            part.CopyTo(result, offset);
            offset += part.Length;
        }

        return result;
    }

    private protected static byte[] GoodHeader() => [0x53, 0x4A, 0x52, 0x4E, JournalFraming.Version];

    private protected static async Task<bool> FileEqualsAsync(string path, byte[] expected, CancellationToken cancellationToken) =>
        (await File.ReadAllBytesAsync(path, cancellationToken)).AsSpan().SequenceEqual(expected);

    private protected static bool IsSingleRepair(IReadOnlyList<JournalRepair> repairs, string path, JournalRepairKind kind, long originalLength, long discardedBytes) =>
        repairs.Count == 1 && repairs[0] == new JournalRepair(path, kind, originalLength, discardedBytes);

    private protected static byte[] OversizedLength() => [0xFF, 0xFF, 0xFF, 0x7F];

    private protected static State NewManifest() => new() { Format = 1, CurrentJournal = 1, NextSequence = 1 };

    private protected static PersistenceOptions NewPersistence(string dataDir) => new() { DataDir = dataDir, JournalMaxSegmentMb = 16, FlushInterval = 5 };

    private protected byte[] BuildFrame(ulong sequence, string key)
    {
        var scratch = NodePathKit.Combine(Dir, "frame-scratch.bin");
        BinaryJournalTestSegmentWriter.WriteSegment(scratch, BinaryJournalTestSegmentWriter.BuildPutRecord(sequence, key, "v"));
        byte[] bytes;
        using (var handle = File.OpenHandle(scratch))
        {
            bytes = new byte[RandomAccess.GetLength(handle)];
            _ = RandomAccess.Read(handle, bytes, 0);
        }

        File.Delete(scratch);
        return bytes[JournalFraming.FileHeaderSize..];
    }

    private protected int CountRecords(int segmentIndex)
    {
        var count = 0;
        using var records = JournalReadPath.ReadAll(Dir, segmentIndex, CancellationToken.None);
        while (records.MoveNext())
            count++;

        return count;
    }

    private protected IReadOnlyList<JournalRepair> Prepare() => JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(NewManifest(), NewPersistence(Dir));

    private protected InvalidDataException Repair() =>
        NodeExceptionAssert.For<InvalidDataException>().Throws(
            (Manifest: NewManifest(), Persistence: NewPersistence(Dir)),
            static state => JournalRecoveryScan.PrepareActiveSegmentForSequenceScan(state.Manifest, state.Persistence));

    private protected async Task<string> WriteSegmentFileAsync(int segmentIndex, byte[] content, CancellationToken cancellationToken)
    {
        var path = BinaryJournalTestSegmentWriter.SegmentPath(Dir, segmentIndex);
        await File.WriteAllBytesAsync(path, content, cancellationToken);
        return path;
    }
}
