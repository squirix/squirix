using System;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Replay behavior when journal segment bytes end mid-frame or fail CRC / decode.</summary>
[Immutable]
public sealed class JournalTruncatedSegmentReplayTests : IsolatedStorageTestBase
{
    /// <summary>Verifies replay failure reporting is non-destructive: reading malformed frames does not mutate segment bytes.</summary>
    [Fact]
    public async Task MalformedFrameLeavesSegmentFileIntact()
    {
        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k", "v");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, record);

        var original = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        var bytes = new byte[original.Length];
        original.CopyTo(bytes);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);
        var mutatedBeforeRead = await File.ReadAllBytesAsync(path, DefaultCancellationToken);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            Dir.Path,
            static dataDirectory =>
            {
                using var records = JournalReadPath.ReadAll(dataDirectory, 1, DefaultCancellationToken);
                while (records.MoveNext())
                    _ = records.Current;
            });
        Assert.Equal(mutatedBeforeRead, await File.ReadAllBytesAsync(path, DefaultCancellationToken));
    }

    /// <summary>CRC mismatch throws <see cref="InvalidDataException" /> to surface corruption.</summary>
    [Fact]
    public async Task ReadAllThrowsOnCrcMismatch()
    {
        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k", "v");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, record);

        var bytes = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);

        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            Dir.Path,
            static dataDirectory =>
            {
                using var records = JournalReadPath.ReadAll(dataDirectory, 1, DefaultCancellationToken);
                while (records.MoveNext())
                    _ = records.Current;
            });
        Assert.Contains("corruption", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies the first complete frame is yielded and enumeration stops when a trailing frame is torn (CRC no longer matches).</summary>
    [Fact]
    public async Task TruncatedSecondCrcYieldsFirstOnly()
    {
        var first = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k1", "a");
        var second = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(2UL, "k2", "b");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, [first, second]);

        using (var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            RandomAccess.SetLength(handle, RandomAccess.GetLength(handle) - 1);

        var list = new List<JournalRecord>(2);
        using var records = JournalReadPath.ReadAll(Dir, 1, DefaultCancellationToken);
        while (records.MoveNext())
            list.Add(records.Current);

        _ = Assert.Single(list);
        Assert.Equal(JournalOperationKind.Put, list[0].Operation);
        Assert.Equal("k1", list[0].Key.Key);
    }
}
