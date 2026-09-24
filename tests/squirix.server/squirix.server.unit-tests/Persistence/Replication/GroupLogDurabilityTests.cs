using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Durable file-handle behavior of the replica-group log replacement path.</summary>
public sealed class GroupLogDurabilityTests : ServerUnitTestBase
{
    /// <summary>Replacement deletes the temp file and detaches the previous durable handle when publication fails mid-way.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplaceCleansUpTempWhenPublicationFails(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-publish-fail");
        var finalPath = Path.Join(dir, "existing-directory");
        Directory.CreateDirectory(finalPath);
        var tempPath = Path.Join(dir, "group.log.tmp");
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);
        var oldPath = Path.Join(dir, "old-group.log");

        using var durability = new GroupLogDurability();

        // A live pre-replacement handle makes the detach observable: without it, a leaked stale handle would be
        // indistinguishable from the expected closed-handle state.
        durability.Open(oldPath, 0L);
        _ = NodeExceptionAssert.For<IOException>().ThrowsAny((durability, tempPath, finalPath), static state => state.durability.Replace(state.tempPath, state.finalPath, 3L));

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(durability.Flush);

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
    }

    /// <summary>Replacement publication leaves the durable handle attached to the new log.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplacePublishesAndReopensTheReplacement(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-replace");
        var finalPath = Path.Join(dir, "group.log");
        var tempPath = Path.Join(dir, "group.log.tmp");
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);

        using var durability = new GroupLogDurability();
        durability.Replace(tempPath, finalPath, 3L);

        // The durable handle must now point at the replacement, so a flush succeeds.
        durability.Flush();

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
        using var published = File.OpenHandle(finalPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        var content = new byte[RandomAccess.GetLength(published)];
        var offset = 0L;
        _ = await Assert.That(HandleEx.TryReadExact(published, content, ref offset)).IsTrue();
        await SequenceAssert.EqualAsync<byte>([1, 2, 3], content);
    }

    /// <summary>Replacement refuses a path without a containing directory and cleans up the temp file.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReplaceRefusesPathWithoutDirectory(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-log-durability-no-dir");
        var tempPath = Path.Join(dir, "group.log.tmp");
        await File.WriteAllBytesAsync(tempPath, ReadOnlyMemory<byte>.Of(1, 2, 3), cancellationToken);

        using var durability = new GroupLogDurability();
        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws((durability, tempPath), static state => state.durability.Replace(state.tempPath, "standalone.log", 3L));

        _ = await Assert.That(File.Exists(tempPath)).IsFalse();
    }
}
