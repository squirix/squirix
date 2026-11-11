using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Checks that simulated torn writes keep only the requested prefix.</summary>
[Immutable]
public sealed class GroupSnapshotFileTests : IsolatedStorageTestBase
{
    private const string GroupId = "torn-snapshot";

    /// <summary>Truncation preserves the prefix and never extends a slot.</summary>
    /// <param name="length">The length to retain.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(4)]
    [Arguments(int.MaxValue)]
    public async Task TearKeepsPrefix(int length, CancellationToken cancellationToken)
    {
        byte[] original = [1, 2, 3, 4];
        await GroupSnapshotFile.WriteImageAsync(Dir, GroupId, original, false, cancellationToken);
        var path = GroupSnapshotFile.SlotPath(Dir, GroupId, 0);

        await GroupSnapshotFile.TearAsync(Dir, GroupId, 0, length, cancellationToken);

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(original.AsSpan(0, Math.Min(length, original.Length)))).IsTrue();
    }

    /// <summary>A negative length is rejected before the slot is changed.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NegativeLengthLeavesSlotUnchanged(CancellationToken cancellationToken)
    {
        byte[] original = [1, 2, 3, 4];
        await GroupSnapshotFile.WriteImageAsync(Dir, GroupId, original, false, cancellationToken);
        var path = GroupSnapshotFile.SlotPath(Dir, GroupId, 0);

        _ = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws((Dir, cancellationToken), static state =>
            _ = GroupSnapshotFile.TearAsync(state.Dir, GroupId, 0, -1, state.cancellationToken));

        var actual = await File.ReadAllBytesAsync(path, cancellationToken);
        _ = await Assert.That(actual.AsSpan().SequenceEqual(original)).IsTrue();
    }

    /// <summary>A missing slot is not created by a simulated tear.</summary>
    [Test]
    public async Task MissingSlotIsNotCreated()
    {
        var directory = GroupStoragePaths.GetGroupDirectory(Dir, GroupId);
        _ = Directory.CreateDirectory(directory);

        _ = NodeExceptionAssert.For<FileNotFoundException>().Throws(Dir, static dir =>
            _ = GroupSnapshotFile.TearAsync(dir, GroupId, 0, 1, CancellationToken.None));

        _ = await Assert.That(File.Exists(GroupSnapshotFile.PointerPath(Dir, GroupId))).IsFalse();
        _ = await Assert.That(Directory.GetFiles(directory, "group.checkpoint-*.snapshot")).IsEmpty();
    }
}
