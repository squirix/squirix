using System;
using System.IO;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Durable file-handle behavior of the replica-group log replacement path.</summary>
public sealed class GroupLogDurabilityTests : ServerUnitTestBase
{
    /// <summary>Replacement deletes the temp file and detaches the previous durable handle when publication fails mid-way.</summary>
    [Fact]
    public void ReplaceCleansUpTempWhenPublicationFails()
    {
        using var dir = new TempDirectory("squirix-log-durability-publish-fail");
        var finalPath = Path.Join(dir.Path, "existing-directory");
        Directory.CreateDirectory(finalPath);
        var tempPath = Path.Join(dir.Path, "group.log.tmp");
        File.WriteAllBytes(tempPath, [1, 2, 3]);
        var oldPath = Path.Join(dir.Path, "old-group.log");

        using var durability = new GroupLogDurability();

        // A live pre-replacement handle makes the detach observable: without it, a leaked stale handle would be
        // indistinguishable from the expected closed-handle state.
        durability.Open(oldPath, 0L);
        _ = NodeExceptionAssert.For<IOException>().ThrowsAny((durability, tempPath, finalPath), static state => state.durability.Replace(state.tempPath, state.finalPath, 3L));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(durability.Flush);

        Assert.False(File.Exists(tempPath));
    }

    /// <summary>Replacement publication leaves the durable handle attached to the new log.</summary>
    [Fact]
    public void ReplacePublishesAndReopensTheReplacement()
    {
        using var dir = new TempDirectory("squirix-log-durability-replace");
        var finalPath = Path.Join(dir.Path, "group.log");
        var tempPath = Path.Join(dir.Path, "group.log.tmp");
        File.WriteAllBytes(tempPath, [1, 2, 3]);

        using var durability = new GroupLogDurability();
        durability.Replace(tempPath, finalPath, 3L);

        // The durable handle must now point at the replacement, so a flush succeeds.
        durability.Flush();

        Assert.False(File.Exists(tempPath));
        using var published = File.OpenHandle(finalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var content = new byte[RandomAccess.GetLength(published)];
        var offset = 0L;
        Assert.True(HandleEx.TryReadExact(published, content, ref offset));
        Assert.Equal(new byte[] { 1, 2, 3 }, content);
    }

    /// <summary>Replacement refuses a path without a containing directory and cleans up the temp file.</summary>
    [Fact]
    public void ReplaceRefusesPathWithoutDirectory()
    {
        using var dir = new TempDirectory("squirix-log-durability-no-dir");
        var tempPath = Path.Join(dir.Path, "group.log.tmp");
        File.WriteAllBytes(tempPath, [1, 2, 3]);

        using var durability = new GroupLogDurability();
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws((durability, tempPath), static state => state.durability.Replace(state.tempPath, "standalone.log", 3L));

        Assert.False(File.Exists(tempPath));
    }
}
