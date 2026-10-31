using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.Threading;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>An append the follower path accepts wakes the apply loop of its group; a refused one does not.</summary>
[Immutable]
public sealed class ReplicaFollowerSignalTests : ServerUnitTestBase
{
    private const string GroupId = "n2";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    /// <summary>An accepted append raises the apply signal of its group.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptedAppendRaisesSignal(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-append");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1, Batch(1, 0, 0), cancellationToken);

        _ = await Assert.That(result.Success).IsTrue();
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsEqualTo(TimedWaitResult.Signaled);
    }

    /// <summary>An append the log refuses leaves the apply signal idle.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RefusedAppendLeavesSignalIdle(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-signal-refused");
        await using var registry = await OpenAsync(dir, cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        var result = await follower.AppendAsync(GroupId, Fingerprint, 1, Batch(2, 1, 0), cancellationToken);

        _ = await Assert.That(result.Success).IsFalse();
        _ = await Assert.That(await IsRaisedAsync(registry, cancellationToken)).IsEqualTo(TimedWaitResult.TimedOut);
    }

    private static async Task<ReplicaGroupRegistry> OpenAsync(string dir, CancellationToken cancellationToken)
    {
        await SeededLogs.SeedGroupsAsync(dir, ["n1", GroupId], cancellationToken);
        var registry = new ReplicaGroupRegistry(dir, ["n1", GroupId], 3, Fingerprint, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        try
        {
            await registry.OpenAsync(cancellationToken);
        }
        catch
        {
            await registry.DisposeAsync();
            throw;
        }

        return registry;
    }

    /// <summary>Takes the pending notification of the group without waiting.</summary>
    /// <param name="registry">The registry of the group.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>The signaled result when a notification was pending.</returns>
    private static Task<TimedWaitResult> IsRaisedAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken) =>
        registry.ApplySignalFor(GroupId).WaitAsync(TimeSpan.Zero, TimeProvider.System, cancellationToken);

    private static FollowerBatch Batch(ulong logIndex, ulong prevIndex, ulong commitIndex)
    {
        var record = new ReplicaLogRecord(
            logIndex,
            1,
            $"op-{logIndex}",
            "cache",
            new byte[] { 1 },
            "UserMutation",
            "cache",
            Encoding.UTF8.GetBytes("k"),
            "Set",
            Encoding.UTF8.GetBytes("v"),
            ReadOnlyMemory<byte>.Empty,
            0,
            0,
            0);
        return new FollowerBatch([FollowerLogEntry.Of(in record)], "n2", 1, prevIndex, prevIndex == 0 ? 0UL : 1UL, commitIndex);
    }
}
