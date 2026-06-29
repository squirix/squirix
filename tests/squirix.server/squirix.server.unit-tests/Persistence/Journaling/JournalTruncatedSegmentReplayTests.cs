using System;
using System.Buffers;
using System.Collections.Generic;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Journaling;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Observability;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Journaling;

/// <summary>Replay behavior when journal segment bytes end mid-frame or fail CRC / decode.</summary>
public sealed class JournalTruncatedSegmentReplayTests : UnitTestBase
{
    /// <summary>Verifies replay failure reporting is non-destructive: reading malformed frames does not mutate segment bytes.</summary>
    [Fact]
    public async Task ReadAllOnMalformedFrameDoesNotMutateSegmentFile()
    {
        using var dir = new TempDirectory("squirix-journal-readonly-failure");
        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, [record]);

        var original = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        var bytes = ArrayPool<byte>.Shared.Rent(original.Length);
        try
        {
            original.CopyTo(bytes.AsSpan(0, original.Length));
            bytes[original.Length - 1] ^= 0xFF;
            await File.WriteAllBytesAsync(path, bytes.AsMemory(0, original.Length), DefaultCancellationToken);
            var mutatedBeforeRead = await File.ReadAllBytesAsync(path, DefaultCancellationToken);

            _ = Assert.Throws<InvalidDataException>(() =>
            {
                foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
                    _ = unused;
            });
            Assert.Equal(mutatedBeforeRead, await File.ReadAllBytesAsync(path, DefaultCancellationToken));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(bytes);
        }
    }

    /// <summary>CRC mismatch throws <see cref="InvalidDataException" /> to surface corruption.</summary>
    [Fact]
    public async Task ReadAllThrowsOnCrcMismatch()
    {
        using var dir = new TempDirectory("squirix-journal-badcrc");
        var record = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k", "v");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, [record]);

        var bytes = await File.ReadAllBytesAsync(path, DefaultCancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, DefaultCancellationToken);

        var ex = Assert.Throws<InvalidDataException>(() =>
        {
            foreach (var unused in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
                _ = unused;
        });
        Assert.Contains("ChecksumMismatch", ex.Message, StringComparison.InvariantCulture);
    }

    /// <summary>Verifies the first complete frame is yielded and enumeration stops when a trailing frame is torn (CRC no longer matches).</summary>
    [Fact]
    public async Task ReadAllYieldsFirstFrameWhenSecondFrameCrcIsTruncated()
    {
        using var dir = new TempDirectory("squirix-journal-trunc");
        var first = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(1UL, "k1", "a");
        var second = await BinaryJournalTestSegmentWriter.BuildPutRecordAsync(2UL, "k2", "b");
        var path = PathKit.Combine(dir, $"{StorageFilePrefixes.Journal}000001{StorageFileExtensions.Journal}");
        await BinaryJournalTestSegmentWriter.WriteSegmentAsync(path, [first, second]);

        await using (var fs = new FileStream(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            fs.SetLength(fs.Length - 1);

        var list = new List<JournalRecord>(2);
        foreach (var record in JournalReader.ReadAll(dir, 1, DefaultCancellationToken))
            list.Add(record);

        _ = Assert.Single(list);
        Assert.Equal(JournalOperationKind.Put, list[0].Operation);
        Assert.Equal("k1", list[0].Key.Key);
    }
}
