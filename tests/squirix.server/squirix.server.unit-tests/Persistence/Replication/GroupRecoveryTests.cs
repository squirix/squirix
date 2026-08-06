using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Lifecycle of the group recovery coordinator over the local composition.</summary>
public sealed class GroupRecoveryTests : ServerUnitTestBase
{
    /// <summary>RecoverAllAsync can be invoked more than once; prior logs are disposed before reopening.</summary>
    [Fact]
    public async Task RecoverAllAsyncCanRunTwice()
    {
        using var dir = new TempDirectory("squirix-group-recovery-twice");
        var recovery = new GroupRecovery(dir, GroupComposition.Create(["grp-1", "grp-2"]));

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        Assert.NotNull(recovery.GetLog("grp-1"));
        Assert.NotNull(recovery.GetLog("grp-2"));

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        Assert.NotNull(recovery.GetLog("grp-1"));
        Assert.NotNull(recovery.GetLog("grp-2"));
    }

    /// <summary>When one group fails to recover, previously opened logs are disposed and the error propagates.</summary>
    [Fact]
    public async Task FailedGroupRecoveryDisposesOpenedLogsAndRollsBack()
    {
        using var dir = new TempDirectory("squirix-group-recovery-failure");
        var recovery = new GroupRecovery(dir, GroupComposition.Create(["grp-1", "grp-2"]));
        await recovery.RecoverAllAsync(DefaultCancellationToken);

        // Corrupt grp-2 metadata so the next recovery attempt fails mid-loop.
        await File.WriteAllTextAsync(GroupStoragePaths.GetMetadataPath(dir, "grp-2"), "corrupt", DefaultCancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(recovery.RecoverAllAsync(DefaultCancellationToken));

        // The partial state is rolled back.
        Assert.Null(recovery.GetLog("grp-1"));

        // After the corrupt group is removed, a fresh recovery succeeds end-to-end.
        Directory.Delete(GroupStoragePaths.GetGroupDirectory(dir, "grp-2"), true);

        await recovery.RecoverAllAsync(DefaultCancellationToken);
        Assert.NotNull(recovery.GetLog("grp-1"));
        Assert.NotNull(recovery.GetLog("grp-2"));
    }
}
