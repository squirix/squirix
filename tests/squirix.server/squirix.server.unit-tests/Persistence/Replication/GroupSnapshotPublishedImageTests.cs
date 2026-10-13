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

/// <summary>The encoded published image of a group snapshot holds exactly the bytes the store writes to its file.</summary>
public sealed class GroupSnapshotPublishedImageTests : ServerUnitTestBase
{
    private const string GroupId = "n2";

    /// <summary>The encoded image equals the published file byte for byte, and the reader decodes it to the snapshot.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EncodedImageEqualsPublishedFile(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-snapshot-image");
        Directory.CreateDirectory(Path.GetDirectoryName(GroupStoragePaths.GetSnapshotPath(dir, GroupId))!);
        var record = new GroupIdempotencyRecord("scope", "op-1", new byte[] { 1, 2 }, new byte[] { 3 }, GroupRecordKind.UserMutation, DateTime.UnixEpoch, DateTime.UnixEpoch, 3UL, 2UL);
        var snapshot = new GroupSnapshot(GroupId, new byte[] { 9, 8, 7 }, 4UL, 2UL, 3UL, 5UL, [record], DateTime.UnixEpoch.AddSeconds(1));
        var store = new GroupSnapshotStore(dir, GroupId);

        await store.PublishAsync(snapshot, cancellationToken);
        var file = await File.ReadAllBytesAsync(GroupStoragePaths.GetSnapshotPath(dir, GroupId), cancellationToken);
        var image = new byte[GroupSnapshotStore.ComputePublishedLength(in snapshot, GroupSnapshotStore.DefaultMaxSnapshotBytes)];
        GroupSnapshotStore.EncodePublished(image, in snapshot);

        await SequenceAssert.EqualAsync(file, image);
        _ = await Assert.That(GroupSnapshotStore.TryDecodePublished(image, GroupSnapshotStore.DefaultMaxSnapshotBytes, out var decoded)).IsTrue();
        _ = await Assert.That((decoded.GroupId, decoded.CommitIndex, decoded.CommittedOutcomes.Count)).IsEqualTo((GroupId, 5UL, 1));
    }
}
