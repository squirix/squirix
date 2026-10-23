using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Group logs record the topology they were written for, and a log of another topology is refused when the registry opens.</summary>
[Immutable]
public sealed class ReplicaGroupRegistryTopologyTests : ServerUnitTestBase
{
    private const string GroupId = "node-a";

    private static readonly byte[] Written = [9];

    private static readonly byte[] Changed = [7];

    /// <summary>Opening a fresh group log durably records the configured fingerprint and generation, and a reopen under the same topology succeeds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OpenRecordsConfiguredTopology(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-registry-topology-record");
        await using (var first = new ReplicaGroupRegistry(dir, [GroupId], 1, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System })
            await first.OpenAsync(cancellationToken);

        await using var reopened = new ReplicaGroupRegistry(dir, [GroupId], 1, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        await reopened.OpenAsync(cancellationToken);
        _ = reopened.TryGetLog(GroupId, out var log);

        var status = await log!.GetStatusAsync(cancellationToken);

        await SequenceAssert.EqualAsync(Written, status.TopologyFingerprint.ToArray());
        _ = await Assert.That(status.ConfigurationGeneration).IsEqualTo(1UL);
    }

    /// <summary>A fresh complete copy starts with every slot ready, and an unseeded copy starts with every slot unverified.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OnlyCompleteCopySlotsStartReady(CancellationToken cancellationToken)
    {
        using var completeDir = new TempDirectory("squirix-registry-slots-complete");
        using var unseededDir = new TempDirectory("squirix-registry-slots-unseeded");
        await SeededLogs.SeedGroupsAsync(completeDir, [GroupId], cancellationToken);
        await using var complete = new ReplicaGroupRegistry(completeDir, [GroupId], 3, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        await using var unseeded = new ReplicaGroupRegistry(unseededDir, [GroupId], 3, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };

        await complete.OpenAsync(cancellationToken);
        await unseeded.OpenAsync(cancellationToken);

        _ = await Assert.That((complete.EligibilityFor(GroupId).AllCanCountInWriteQuorum(), unseeded.EligibilityFor(GroupId).AllCanCountInWriteQuorum())).IsEqualTo((true, false));
    }

    /// <summary>A log written under one topology is refused when the registry reopens it under another, before anything is applied from it.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ChangedTopologyIsRefused(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-registry-topology-refuse");
        await SeededLogs.SeedGroupsAsync(dir, [GroupId], cancellationToken);
        await using (var written = new ReplicaGroupRegistry(dir, [GroupId], 1, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System })
        {
            await written.OpenAsync(cancellationToken);
            _ = written.TryGetLog(GroupId, out var writtenLog);
            var entry = new FollowerLogEntry(1, 1, ReadOnlyMemory<byte>.Of(1, 2, 3));
            _ = await writtenLog!.AppendAsync(new FollowerLogAppendRequest(GroupId, 1, 0, 0, 0, ReadOnlyMemory<FollowerLogEntry>.Of(entry)), cancellationToken);
        }

        await using var changed = new ReplicaGroupRegistry(dir, [GroupId], 1, Changed, 2, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };

        var exception = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(changed.OpenAsync(cancellationToken));
        await using var original = new ReplicaGroupRegistry(dir, [GroupId], 1, Written, 1, NullLoggerFactory.Instance, new FollowerLogOptions { TimeProvider = TimeProvider.System, ShutdownTimeProvider = TimeProvider.System }) { ElectionClock = TimeProvider.System };
        await original.OpenAsync(cancellationToken);
        _ = original.TryGetLog(GroupId, out var log);
        var status = await log!.GetStatusAsync(cancellationToken);

        _ = await Assert.That(exception.Message).Contains("was written for a different topology", StringComparison.Ordinal);
        await SequenceAssert.EqualAsync(Written, status.TopologyFingerprint.ToArray());
        _ = await Assert.That(status.LastLogIndex).IsEqualTo(1UL);
    }
}
