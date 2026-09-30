using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Journaling.Read;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Journaling.Recovery;

/// <summary>File handle ownership of the journal replay enumerator.</summary>
[Immutable]
public sealed class JournalReadPathTests : IsolatedStorageTestBase
{
    /// <summary>Reading the current record of a disposed replay enumerator reports the disposal.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAllCurrentAfterDisposeThrows(CancellationToken cancellationToken)
    {
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v"));

        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        _ = await Assert.That(records.MoveNext()).IsTrue();

        // ReSharper disable once DisposeOnUsingVariable — intentional early dispose: the test asserts Current refuses to run afterwards.
        records.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(records, static r => _ = r.Current);
    }

    /// <summary>Reading the current record before the first move reports an unpositioned enumerator.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public void ReadAllCurrentBeforeMoveNextThrows(CancellationToken cancellationToken)
    {
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v"));

        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(records, static r => _ = r.Current);
    }

    /// <summary>A disposed replay enumerator releases its segment and refuses to move on instead of opening the next one.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAllMoveNextAfterDisposeThrows(CancellationToken cancellationToken)
    {
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 1, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k1", "a"));
        BinaryJournalTestSegmentWriter.WriteJournalSegment(Dir, 2, BinaryJournalTestSegmentWriter.BuildPutRecord(2UL, "k2", "b"));

        using var records = JournalReadPath.ReadAll(Dir, 1, cancellationToken);
        _ = await Assert.That(records.MoveNext()).IsTrue();

        // ReSharper disable once DisposeOnUsingVariable — intentional early dispose: the test asserts MoveNext refuses to run afterwards.
        records.Dispose();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(records, static r => _ = r.MoveNext());
        using var first = File.OpenHandle(BinaryJournalTestSegmentWriter.SegmentPath(Dir, 1), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        using var second = File.OpenHandle(BinaryJournalTestSegmentWriter.SegmentPath(Dir, 2), FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        _ = await Assert.That(first.IsInvalid || second.IsInvalid).IsFalse();
    }

    /// <summary>A segment rejected for an invalid header leaves no open handle behind, so the file can be taken exclusively right away.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReadAllReleasesSegmentOnBadHeader(CancellationToken cancellationToken)
    {
        var path = BinaryJournalTestSegmentWriter.SegmentPath(Dir, 1);
        BinaryJournalTestSegmentWriter.WriteSegment(path, BinaryJournalTestSegmentWriter.BuildPutRecord(1UL, "k", "v"));
        BinaryJournalTestSegmentWriter.SetHeaderVersion(path, byte.MaxValue);

        _ = NodeExceptionAssert.For<InvalidDataException>().Throws(
            Dir,
            cancellationToken,
            static (dataDirectory, token) =>
            {
                using var records = JournalReadPath.ReadAll(dataDirectory, 1, token);
                while (records.MoveNext())
                    _ = records.Current;
            });

        using var exclusive = File.OpenHandle(path, FileMode.Open, FileAccess.ReadWrite, FileShare.None);
        _ = await Assert.That(exclusive.IsInvalid).IsFalse();
    }
}
