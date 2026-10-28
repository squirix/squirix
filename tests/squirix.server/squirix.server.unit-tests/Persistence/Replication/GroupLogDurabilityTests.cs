using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Durable file-handle behavior of the replica-group log generation files.</summary>
public sealed class GroupLogDurabilityTests : ServerUnitTestBase
{
    /// <summary>Adopting a sealed file makes it the live log and empties the file it supersedes.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdoptMovesTheLiveLogAndEmptiesTheOldFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-adopt");
        var oldPath = Path.Join(dir, "group.log.0");
        var newPath = Path.Join(dir, "group.log.1");
        await File.WriteAllBytesAsync(newPath, ReadOnlyMemory<byte>.Of(9, 9), cancellationToken);
        using var durability = new GroupLogDurability();
        durability.Open(oldPath, 10L);

        var standby = GroupLogDurability.OpenStandby(newPath);
        try
        {
            GroupLogDurability.Seal(standby, 2UL);
            var sealedHandle = standby;
            standby = null;
            durability.Adopt(sealedHandle);
        }
        finally
        {
            standby?.Dispose();
        }

        durability.Write(ReadOnlyMemory<byte>.Of(7), GroupLogSlots.HeaderSize);
        durability.Flush();

        _ = await Assert.That(new FileInfo(oldPath).Length).IsEqualTo(0L);
        _ = await Assert.That(new FileInfo(newPath).Length).IsEqualTo(GroupLogSlots.HeaderSize + 1L);
    }

    /// <summary>Creating the first log seals generation 1 into the file and leaves it as the live log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CreateSealsTheFirstGeneration(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-create");
        var path = Path.Join(dir, "group.log.0");
        using (var durability = new GroupLogDurability())
        {
            durability.Create(path, 1UL);
            durability.Flush();
        }

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.Length).IsEqualTo(GroupLogSlots.HeaderSize);
        _ = await Assert.That(GroupLogSlots.Select(actual, actual.Length, [], -1L, "dir")).IsEqualTo((0, 1UL));
    }

    /// <summary>Appending exactly at the known end of the file does not size the file again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SequentialAppendsDoNotSizeTheFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-append");
        var path = Path.Join(dir, "group.log.0");
        long sizings;
        using (var durability = new GroupLogDurability())
        {
            durability.Open(path, GroupLogSlots.HeaderSize);
            durability.Write(ReadOnlyMemory<byte>.Of(1, 2, 3), GroupLogSlots.HeaderSize);
            durability.Write(ReadOnlyMemory<byte>.Of(4, 5), GroupLogSlots.HeaderSize + 3L);
            durability.Write(ReadOnlyMemory<byte>.Of(6), GroupLogSlots.HeaderSize + 5L);
            durability.Flush();
            sizings = durability.SetLengthCount;
        }

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(sizings).IsEqualTo(0L);
        _ = await Assert.That(actual.Length).IsEqualTo(GroupLogSlots.HeaderSize + 6);
        _ = await Assert.That(actual.AsSpan(GroupLogSlots.HeaderSize).SequenceEqual(ReadOnlyMemory<byte>.Of(1, 2, 3, 4, 5, 6).Span)).IsTrue();
    }

    /// <summary>A write that starts before the end of the file cuts the stale suffix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task WritingBeforeTheEndCutsTheStaleSuffix(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-stale");
        var path = Path.Join(dir, "group.log.0");
        using (var durability = new GroupLogDurability())
        {
            durability.Open(path, 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(1, 2, 3), 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(4, 5, 6), 3L);
            durability.Write(ReadOnlyMemory<byte>.Of(7), 3L);
            durability.Flush();
        }

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(ReadOnlyMemory<byte>.Of(1, 2, 3, 7).Span)).IsTrue();
    }

    /// <summary>A failed write leaves the length unknown, so the next write at the former end still sizes the file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedWriteMakesTheNextWriteSizeTheFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-failed");
        var path = Path.Join(dir, "group.log.0");
        long before;
        long after;
        using (var durability = new GroupLogDurability())
        {
            durability.Open(path, 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(1, 2, 3), 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(4, 5, 6), 3L);
            before = durability.SetLengthCount;

            _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(durability, static d => d.Write(ReadOnlyMemory<byte>.Of(9), -1L));
            durability.Write(ReadOnlyMemory<byte>.Of(7), 3L);
            durability.Flush();
            after = durability.SetLengthCount;
        }

        _ = await Assert.That(after).IsEqualTo(before + 1L);
        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(4L);
        _ = await Assert.That((await File.ReadAllBytesAsync(path, cancellationToken)).AsSpan().SequenceEqual(ReadOnlyMemory<byte>.Of(1, 2, 3, 7).Span)).IsTrue();
    }

    /// <summary>After a truncate the next write skips sizing at the new end and sizes the file when it starts before it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task TruncateSetsTheKnownLength(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-truncate");
        var path = Path.Join(dir, "group.log.0");
        long before;
        long atEnd;
        long after;
        using (var durability = new GroupLogDurability())
        {
            durability.Open(path, 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(1, 2, 3, 4, 5, 6), 0L);
            durability.Truncate(4L);
            before = durability.SetLengthCount;

            durability.Write(ReadOnlyMemory<byte>.Of(7), 4L);
            atEnd = durability.SetLengthCount;
            durability.Write(ReadOnlyMemory<byte>.Of(8), 2L);
            durability.Flush();
            after = durability.SetLengthCount;
        }

        _ = await Assert.That(atEnd).IsEqualTo(before);
        _ = await Assert.That(after).IsEqualTo(before + 1L);
        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(ReadOnlyMemory<byte>.Of(1, 2, 8).Span)).IsTrue();
    }

    /// <summary>Reopening a log forgets the known length, so the first write sizes the file.</summary>
    [Test]
    public async Task ReopenStartsWithUnknownLength()
    {
        using var dir = new TempDirectory("squirix-log-durability-reopen");
        var path = Path.Join(dir, "group.log.0");
        long before;
        long after;
        using (var durability = new GroupLogDurability())
        {
            durability.Open(path, 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(1, 2, 3), 0L);
            durability.Write(ReadOnlyMemory<byte>.Of(4, 5, 6), 3L);
            durability.Open(path, 6L);
            before = durability.SetLengthCount;

            durability.Write(ReadOnlyMemory<byte>.Of(7), 3L);
            durability.Flush();
            after = durability.SetLengthCount;
        }

        _ = await Assert.That(after).IsEqualTo(before + 1L);
        _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(4L);
    }

    /// <summary>Opening a standby file empties whatever an earlier generation left in it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenStandbyEmptiesAnExistingFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-standby");
        var path = Path.Join(dir, "group.log.1");
        await File.WriteAllBytesAsync(path, ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);

        using (GroupLogDurability.OpenStandby(path))
            _ = await Assert.That(new FileInfo(path).Length).IsEqualTo(0L);
    }
}
