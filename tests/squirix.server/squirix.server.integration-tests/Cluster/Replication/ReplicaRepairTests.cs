using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Cluster.Replication;

/// <summary>Follower repair ordering, eligibility, and lifecycle integration.</summary>
public sealed class ReplicaRepairTests : NodeIntegrationTestBase
{
    private const string GroupId = "repair-group";

    /// <summary>A restarted replica recorded as catching up stays out of the quorum until it is marked ready.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RestartedReplicaRemainsExcluded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-repair-restart");
        await using (var log = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await Assert.That((await log.AppendAsync(Append(1UL, 1UL, "one"), cancellationToken)).Success).IsTrue();
        }

        await using var reopened = new FollowerLog(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
        await reopened.OpenAsync(cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        var fingerprint = new byte[] { 1, 2, 3 };
        var recorded = Progress(status.LastLogIndex, status.CommitIndex, 1UL, fingerprint);
        var expected = Progress(2UL, 0UL, 2UL, fingerprint);
        var eligibility = new ReplicaEligibility(3);
        var quorum = new ReplicaCommitQuorum(3, 2UL, eligibility);

        _ = await Assert.That(eligibility.TryMarkReady(0, in expected)).IsTrue();
        _ = await Assert.That(eligibility.TryMarkCatchingUp(1, in recorded)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0UL, 2UL)).IsEqualTo(0UL);

        _ = await Assert.That(eligibility.TryMarkReady(1, in expected)).IsTrue();
        _ = await Assert.That(quorum.FindCommitIndex(0UL, 2UL)).IsEqualTo(2UL);
    }

    private static FollowerLogAppendRequest Append(ulong index, ulong term, string payload) => new(
        "leader",
        term,
        index - 1UL,
        index == 1UL ? 0UL : 1UL,
        0UL,
        ReadOnlyMemory<FollowerLogEntry>.Of(Entry(index, term, payload)));

    private static FollowerLogEntry Entry(ulong index, ulong term, string payload) => new(index, term, Encoding.UTF8.GetBytes(payload));

    private static ReplicaProgress Progress(ulong matchIndex, ulong commitIndex, ulong lastTerm, byte[] fingerprint) => new(
        matchIndex + 1UL,
        matchIndex,
        commitIndex,
        lastTerm,
        fingerprint,
        1UL);
}
