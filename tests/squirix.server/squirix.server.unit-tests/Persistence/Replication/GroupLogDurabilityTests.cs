using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Win32.SafeHandles;
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

        SafeFileHandle? standby = GroupLogDurability.OpenStandby(newPath);
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
