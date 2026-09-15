using System;
using System.Collections.Generic;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Abstractions;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>Replay behavior when journal segment bytes end mid-frame or fail CRC / decode.</summary>
[Immutable]
public sealed class JournalTruncatedSegmentReplayTests : IsolatedStorageTestBase
{
    /// <summary>Verifies replay failure reporting is non-destructive: reading malformed frames does not mutate segment bytes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MalformedFrameLeavesSegmentFileIntact(CancellationToken cancellationToken)
    {
        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        BinaryJournalTestSegmentWriter.WriteSegment(path, record);

        var original = await File.ReadAllBytesAsync(path, cancellationToken);
        var bytes = new byte[original.Length];
        original.CopyTo(bytes);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        var mutatedBeforeRead = await File.ReadAllBytesAsync(path, cancellationToken);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            Dir.Path,
            cancellationToken,
            static (dataDirectory, token) =>
            {
                using var records = JournalReadPath.ReadAll(dataDirectory, 1, token);
                while (records.MoveNext())
                    _ = records.Current;
            });
        var pathBytes = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.Equal(mutatedBeforeRead, pathBytes);
    }

    /// <summary>CRC mismatch throws <see cref="InvalidDataException" /> to surface corruption.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAllThrowsOnCrcMismatch(CancellationToken cancellationToken)
    {
        var record = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        BinaryJournalTestSegmentWriter.WriteSegment(path, record);

        var bytes = await File.ReadAllBytesAsync(path, cancellationToken);
        bytes[^1] ^= 0xFF;
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);

        var ex = NodeExceptionAssert.For<InvalidDataException>().Throws(
            Dir.Path,
            cancellationToken,
            static (dataDirectory, token) =>
            {
                using var records = JournalReadPath.ReadAll(dataDirectory, 1, token);
                while (records.MoveNext())
                    _ = records.Current;
            });
        _ = await Assert.That(ex.Message).Contains("corruption", StringComparison.OrdinalIgnoreCase);
    }

    /// <summary>Verifies the first complete frame is yielded and enumeration stops when a trailing frame is torn (CRC no longer matches).</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncatedSecondCrcYieldsFirstOnly(CancellationToken cancellationToken)
    {
        var first = BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k1", "a");
        var second = BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "k2", "b");
        var path = NodePathKit.Combine(Dir, $"{FilePrefixes.Journal}000001{FileExtensions.Journal}");
        BinaryJournalTestSegmentWriter.WriteSegment(path, [first, second]);

        using (var handle = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.ReadWrite))
            RandomAccess.SetLength(handle, RandomAccess.GetLength(handle) - 1);

        var list = new List<JournalRecord>(2);
        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        while (records.MoveNext())
            list.Add(records.Current);

        _ = await Assert.That(list).HasSingleItem();
        _ = await Assert.That(list[0].Operation).IsEqualTo(JournalOperationKind.Put);
        _ = await Assert.That(list[0].Key.Key).IsEqualTo("k1");
    }
}
