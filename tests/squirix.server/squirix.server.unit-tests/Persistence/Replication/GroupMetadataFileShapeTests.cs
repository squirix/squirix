using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>How a metadata file of the wrong shape is diagnosed, and creation beside debris of an interrupted creation.</summary>
[Immutable]
public sealed class GroupMetadataFileShapeTests : ServerUnitTestBase
{
    private const string GroupId = "meta-shape";

    /// <summary>An empty file and a file shorter than the earlier magic are corrupt, and are not modified.</summary>
    /// <param name="length">The file length in bytes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(0)]
    [Arguments(3)]
    public async Task ShortFileIsCorrupt(int length, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-shape-short");
        var path = await WriteFileAsync(dir, new byte[length], cancellationToken);

        using var store = NewStore(dir);
        var loaded = store.Open();

        _ = await Assert.That(loaded.Status).IsEqualTo(GroupMetadataReadStatus.Corrupt);
        _ = await Assert.That((await GroupMetadataFile.ReadBytesAsync(path, cancellationToken)).Length).IsEqualTo(length);
    }

    /// <summary>A file that starts with the earlier magic is from an earlier build whatever its length.</summary>
    /// <param name="length">The file length in bytes.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(4)]
    [Arguments(100)]
    [Arguments(GroupMetadataSlots.FileSize)]
    [Arguments(GroupMetadataSlots.FileSize + 1)]
    public async Task EarlierMagicIsUnsupportedAtAnyLength(int length, CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-shape-earlier");
        var bytes = new byte[length];
        bytes[0] = 0x53;
        bytes[1] = 0x51;
        bytes[2] = 0x52;
        bytes[3] = 0x4D;
        var path = await WriteFileAsync(dir, bytes, cancellationToken);

        using var store = NewStore(dir);
        var loaded = store.Open();

        _ = await Assert.That(loaded.Status).IsEqualTo(GroupMetadataReadStatus.Unsupported);
        var after = await GroupMetadataFile.ReadBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(bytes, after);
    }

    /// <summary>A temp file left beside a missing metadata file, as after a crash during the first creation, does not break creation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StaleTempFileDoesNotBreakCreation(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-meta-shape-stale-temp");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await File.WriteAllBytesAsync(GroupStoragePaths.GetMetadataTempPath(dir, GroupId), [1, 2, 3], cancellationToken);
        using var store = NewStore(dir);

        store.Publish(new GroupLogMetadata(GroupId, ReadOnlyMemory<byte>.Empty, 0UL, 5UL, string.Empty, 0UL, 0UL));

        var read = GroupMetadataStore.Read(path);
        _ = await Assert.That((read.Status, read.Sequence, read.Metadata.CurrentTerm)).IsEqualTo((GroupMetadataReadStatus.Valid, 1UL, 5UL));
        _ = await Assert.That((await GroupMetadataFile.ReadBytesAsync(path, cancellationToken)).Length).IsEqualTo(GroupMetadataSlots.FileSize);
    }

    private static GroupMetadataStore NewStore(string dir) =>
        new(GroupStoragePaths.GetMetadataPath(dir, GroupId), GroupStoragePaths.GetMetadataTempPath(dir, GroupId), GroupId, new CountingFollowerLogFaultHooks(), NullLogger.Instance);

    private static async Task<string> WriteFileAsync(string dir, byte[] bytes, CancellationToken cancellationToken)
    {
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        await File.WriteAllBytesAsync(path, bytes, cancellationToken);
        return path;
    }
}
