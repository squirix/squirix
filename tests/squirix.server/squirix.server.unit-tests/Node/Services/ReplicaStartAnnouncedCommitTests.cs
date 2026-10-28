using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Node.Services;
using Squirix.Server.Storage.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitBatchKit;
using static Squirix.Server.UnitTests.Node.Services.ReplicaCommitterDoubles;

namespace Squirix.Server.UnitTests.Node.Services;

/// <summary>A node that starts leading a group makes the commit index it learned as a follower durable first, so the entries a previous leader committed are not recovered as an uncommitted tail.</summary>
[Immutable]
public sealed class ReplicaStartAnnouncedCommitTests : IsolatedStorageTestBase
{
    private const int Entries = 3;

    /// <summary>The coordinator of the start begins at the announced commit index, with nothing recovered above it, and memory holds the committed entries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StartMakesAnnouncedCommitDurable(CancellationToken cancellationToken)
    {
        var local = RecoveredCache();
        await using var registry = await ReplicaCommitBatchKit.OpenRegistryAsync(Dir, DefaultOptions(), cancellationToken);
        await AppendAnnouncedAsync(registry, cancellationToken);
        await using var committer = CreateCommitter(registry, local, new AcceptingGateway(), TimeProvider.System);
        var before = await GetOwnedLog(registry).GetStatusAsync(cancellationToken);

        var (coordinator, _) = await committer.EnsureStartedAsync(false, cancellationToken);

        var after = await GetOwnedLog(registry).GetStatusAsync(cancellationToken);
        _ = await Assert.That((before.CommitIndex, before.CommitHint)).IsEqualTo((0UL, 3UL));
        _ = await Assert.That((after.CommitIndex, coordinator.CommitIndex)).IsEqualTo((3UL, 3UL));
        _ = await Assert.That(coordinator.HasPendingApply).IsFalse();
        _ = await Assert.That(Applied(local)).IsEqualTo("k1,k2,k3");
    }

    private static async Task AppendAnnouncedAsync(ReplicaGroupRegistry registry, CancellationToken cancellationToken)
    {
        var log = GetOwnedLog(registry);
        var factory = new ReplicaMutationFactory(RecoveredCache(), OwnedGroup, 1UL, TimeProvider.System, NullLogger.Instance);
        var entries = new FollowerLogEntry[Entries];
        for (var i = 0; i < entries.Length; i++)
        {
            var index = ulong.CreateChecked(i + 1);
            var key = "k" + (i + 1).ToString(System.Globalization.CultureInfo.InvariantCulture);
            entries[i] = new FollowerLogEntry(index, 1UL, factory.PrepareSet(NewOperationId(), Cache, key, Entry(), index).CanonicalPayload);
        }

        var appended = await log.AppendAsync(new FollowerLogAppendRequest(OwnedGroup, 1UL, 0UL, 0UL, ulong.CreateChecked(Entries), entries), cancellationToken);
        if (!appended.Success)
            throw new InvalidOperationException($"The owned group log refused the entries: {appended.RefusalCode}.");
    }
}
