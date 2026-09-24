using System;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>Lifecycle of the group recovery coordinator over the local composition.</summary>
[Immutable]
public sealed class GroupRecoveryTests : IsolatedStorageTestBase
{
    /// <summary>Acquiring an unknown group or a disposed coordinator returns no lease.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcquireReturnsNullWhenUnusable(CancellationToken cancellationToken)
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1"));
        _ = await Assert.That(recovery.AcquireLog("unknown")).IsNull();

        await recovery.RecoverAllAsync(cancellationToken);
        _ = await Assert.That(recovery.AcquireLog("unknown")).IsNull();

        // ReSharper disable once DisposeOnUsingVariable — intentional early dispose: the test covers Acquire returning null after disposal.
        await recovery.DisposeAsync();
        _ = await Assert.That(recovery.AcquireLog("grp-1")).IsNull();
    }

    /// <summary>When one group fails to recover, previously opened logs are disposed and the error propagates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedRecoveryDisposesLogsAndRollback(CancellationToken cancellationToken)
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1", "grp-2"));
        await recovery.RecoverAllAsync(cancellationToken);

        // Corrupt grp-2 metadata, so the next recovery attempt fails mid-loop.
        await File.WriteAllTextAsync(GroupStoragePaths.GetMetadataPath(Dir, "grp-2"), "corrupt", cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(recovery.RecoverAllAsync(cancellationToken));

        // The partial state is rolled back: no log stays open regardless of which group the composition
        // enumerated first, so the assertion does not depend on FrozenSet<string> ordering.
        _ = await Assert.That(recovery.GetLog("grp-1")).IsNull();
        _ = await Assert.That(recovery.GetLog("grp-2")).IsNull();

        // After the corrupt group is removed, a fresh recovery succeeds end-to-end.
        Directory.Delete(GroupStoragePaths.GetGroupDirectory(Dir, "grp-2"), true);

        await recovery.RecoverAllAsync(cancellationToken);
        _ = await Assert.That(recovery.GetLog("grp-1")).IsNotNull();
        _ = await Assert.That(recovery.GetLog("grp-2")).IsNotNull();
    }

    /// <summary>A log leased across disposal stays usable until the lease is released, then it is disposed of.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LeasedLogSurvivesCloseUntilReleased(CancellationToken cancellationToken)
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1"));
        await recovery.RecoverAllAsync(cancellationToken);

        var lease = recovery.AcquireLog("grp-1");
        _ = await Assert.That(lease).IsNotNull();
        var leased = lease.Log;
        await using (lease)
        {
            // ReSharper disable once DisposeOnUsingVariable — intentional early close: the test covers a leased log surviving coordinator disposal.
            await recovery.DisposeAsync();
            _ = await Assert.That(recovery.GetLog("grp-1")).IsNull();

            // The retired log is not disposed while leased, so fetch-then-use still succeeds.
            var appended = await leased.AppendAsync(AppendRequest(), cancellationToken);
            _ = await Assert.That(appended.Success).IsTrue();
        }

        // The last release disposes of the retired log.
        var rejected = await leased.AppendAsync(AppendRequest(), cancellationToken);
        _ = await Assert.That(rejected.Success).IsFalse();
        _ = await Assert.That(rejected.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);
    }

    /// <summary>RecoverAllAsync can be invoked more than once; prior logs are disposed of before reopening.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RecoverAllAsyncCanRunTwice(CancellationToken cancellationToken)
    {
        await using var recovery = new GroupRecovery(Dir, GroupComposition.Create("grp-1", "grp-2"));

        await recovery.RecoverAllAsync(cancellationToken);
        var firstGrp1 = recovery.GetLog("grp-1");
        _ = await Assert.That(firstGrp1).IsNotNull();
        _ = await Assert.That(recovery.GetLog("grp-2")).IsNotNull();

        // Durable state is present, so the second recovery has something to restore.
        var request = new FollowerLogAppendRequest(
            "leader-1",
            1UL,
            0UL,
            0UL,
            0UL,
            ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("durable"))));
        _ = await firstGrp1.AppendAsync(request, cancellationToken);
        _ = await firstGrp1.AdvanceCommitAsync(1UL, cancellationToken);

        await recovery.RecoverAllAsync(cancellationToken);
        var reopenedGrp1 = recovery.GetLog("grp-1");
        _ = await Assert.That(reopenedGrp1).IsNotNull();
        _ = await Assert.That(recovery.GetLog("grp-2")).IsNotNull();
        _ = await Assert.That(reopenedGrp1).IsNotSameReferenceAs(firstGrp1);

        // The original log was disposed of during recovery and rejects later operations.
        var rejected = await firstGrp1.AppendAsync(request, cancellationToken);
        _ = await Assert.That(rejected.Success).IsFalse();
        _ = await Assert.That(rejected.RefusalCode).IsEqualTo(FollowerLogRefusal.NotReady);

        _ = await Assert.That((await reopenedGrp1.GetStatusAsync(cancellationToken)).LastLogIndex).IsEqualTo(1UL);
        var committed = await recovery.GetCommittedRecordsAsync("grp-1", cancellationToken);
        var only = await Assert.That(committed).HasSingleItem();
        _ = await Assert.That(Encoding.UTF8.GetString(only.Payload.Span)).IsEqualTo("durable");
    }

    private static FollowerLogAppendRequest AppendRequest() => new(
        "leader-1",
        1UL,
        0UL,
        0UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(new FollowerLogEntry(1UL, 1UL, Encoding.UTF8.GetBytes("leased"))));
}
