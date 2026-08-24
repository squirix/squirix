using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Lifecycle of the group recovery coordinator over the local composition.</summary>
[Immutable]
public sealed class GroupRecoveryTests : IsolatedStorageTestBase
{
    /// <summary>When one group fails to recover, previously opened logs are disposed and the error propagates.</summary>
    [Fact]
    public async Task FailedRecoveryDisposesLogsAndRollback()
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1", "grp-2"));
        await recovery.RecoverAllAsync(DefaultCancellationToken);

        // Corrupt grp-2 metadata so the next recovery attempt fails mid-loop.
        await File.WriteAllTextAsync(GroupStoragePaths.GetMetadataPath(Dir, "grp-2"), "corrupt", DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(recovery.RecoverAllAsync(DefaultCancellationToken));

        // The partial state is rolled back: no log stays open regardless of which group the composition
        // enumerated first, so the assertion does not depend on FrozenSet<string> ordering.
        Assert.Null(recovery.GetLog("grp-1"));
        Assert.Null(recovery.GetLog("grp-2"));

        // After the corrupt group is removed, a fresh recovery succeeds end-to-end.
        Directory.Delete(GroupStoragePaths.GetGroupDirectory(Dir, "grp-2"), true);

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        Assert.NotNull(recovery.GetLog("grp-1"));
        Assert.NotNull(recovery.GetLog("grp-2"));
    }

    /// <summary>RecoverAllAsync can be invoked more than once; prior logs are disposed before reopening.</summary>
    [Fact]
    public async Task RecoverAllAsyncCanRunTwice()
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1", "grp-2"));

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        var firstGrp1 = recovery.GetLog("grp-1");
        Assert.NotNull(firstGrp1);
        Assert.NotNull(recovery.GetLog("grp-2"));

        // Durable state is present, so the second recovery has something to restore.
        var request = new FollowerLogAppendRequest(
            "leader-1",
            1UL,
            0UL,
            0UL,
            0UL,
            new ReadOnlyMemory<FollowerLogEntry>([new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("durable"))]));
        _ = await firstGrp1.AppendAsync(request, DefaultCancellationToken);
        _ = await firstGrp1.AdvanceCommitAsync(1UL, DefaultCancellationToken);

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        var reopenedGrp1 = recovery.GetLog("grp-1");
        Assert.NotNull(reopenedGrp1);
        Assert.NotNull(recovery.GetLog("grp-2"));
        Assert.NotSame(firstGrp1, reopenedGrp1);

        // The original log was disposed during recovery and rejects subsequent operations.
        var rejected = await firstGrp1.AppendAsync(request, DefaultCancellationToken);
        Assert.False(rejected.Success);
        Assert.Equal(FollowerLogRefusal.NotReady, rejected.RefusalCode);

        Assert.Equal(1UL, (await reopenedGrp1.GetStatusAsync(DefaultCancellationToken)).LastLogIndex);
        var committed = await recovery.GetCommittedRecordsAsync("grp-1", DefaultCancellationToken);
        var only = Assert.Single(committed);
        Assert.Equal("durable", Encoding.UTF8.GetString(only.Payload.Span));
    }
}
