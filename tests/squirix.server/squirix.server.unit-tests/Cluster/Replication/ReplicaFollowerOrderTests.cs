using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit.IO;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Arrival order of concurrent leader appends at <see cref="ReplicaFollower" />.</summary>
public sealed class ReplicaFollowerOrderTests : ServerUnitTestBase
{
    private const string GroupId = "n1";

    private static readonly byte[] Fingerprint = [9, 8, 7];

    private static readonly TimeSpan HangGuard = TimeSpan.FromSeconds(30);

    /// <summary>Appends queued behind a long gate holder are applied in arrival order instead of one being refused as a gap.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task QueuedAppendsKeepArrivalOrder(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-follower-order");
        using var hooks = new StallableFollowerLogFaultHooks();
        await using var registry = new ReplicaGroupRegistry(dir, [GroupId], 1, Fingerprint, 1, NullLoggerFactory.Instance, new FollowerLogOptions { FaultHooks = hooks });
        await registry.OpenAsync(cancellationToken);
        var follower = new ReplicaFollower(registry, RocksDoubles.CreateReplicaMembers());

        hooks.StallNextFrameWrite();
        var first = follower.AppendAsync(GroupId, Fingerprint, 1, Batch(1, 0), cancellationToken);
        try
        {
            await hooks.Entered.WaitAsync(HangGuard, TimeProvider.System, cancellationToken);
        }
        catch
        {
            hooks.Release();
            throw;
        }

        var second = follower.AppendAsync(GroupId, Fingerprint, 1, Batch(2, 1), cancellationToken);
        var third = follower.AppendAsync(GroupId, Fingerprint, 1, Batch(3, 2), cancellationToken);
        hooks.Release();

        var results = await Task.WhenAll(first, second, third).WaitAsync(HangGuard, TimeProvider.System, cancellationToken);

        _ = await Assert.That(results[0].Success).IsTrue();
        _ = await Assert.That(results[1].Success).IsTrue().Because($"second refused with '{results[1].RefusalCode}'");
        _ = await Assert.That(results[2].Success).IsTrue().Because($"third refused with '{results[2].RefusalCode}'");
        _ = await Assert.That(results[2].LastLogIndex).IsEqualTo(3UL);
    }

    private static FollowerBatch Batch(ulong logIndex, ulong prevIndex)
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
            0,
            0);
        return new FollowerBatch([record], "n1", 1, prevIndex, prevIndex == 0 ? 0UL : 1UL, 0);
    }
}
