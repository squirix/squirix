using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging.Abstractions;
using Squirix.Server.Attributes;
using Squirix.Server.Storage.Replication;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Replication;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Persistence.Replication;

/// <summary>What a follower log reports and keeps as the member state of its copy.</summary>
[Immutable]
public sealed class GroupMemberStateLogTests : ServerUnitTestBase
{
    private const string GroupId = "member-state-log";

    /// <summary>A fresh log opens unseeded and not offered, ready, reports it, and holds it durably.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FreshLogOpensUnseeded(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-state-fresh");

        await using (var log = OpenLog(dir))
        {
            await log.OpenAsync(cancellationToken);

            var status = await log.GetStatusAsync(cancellationToken);
            _ = await Assert.That((status.MemberState, status.BootstrapOffered, status.Readiness)).IsEqualTo((GroupMemberState.Unseeded, false, FollowerLogReadiness.Ready));
        }

        var durable = GroupMetadataStore.Read(GroupStoragePaths.GetMetadataPath(dir, GroupId)).Metadata;
        _ = await Assert.That((durable.MemberState, durable.BootstrapOffered)).IsEqualTo((GroupMemberState.Unseeded, false));
    }

    /// <summary>A term step, a vote, an append, a commit advance and a reopen leave the state and the offer as they were.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StateSurvivesLogOperations(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-state-operations");

        await using (var log = OpenLog(dir))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.ObserveTermAsync(3UL, cancellationToken);
            await AssertCompleteAsync(log, cancellationToken);
            _ = await log.RequestVoteAsync(new ElectionVoteRequest("node-b", 4UL, 0UL, 0UL), cancellationToken);
            await AssertCompleteAsync(log, cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 4UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            await AssertCompleteAsync(log, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);

        await AssertCompleteAsync(reopened, cancellationToken);
        var status = await reopened.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.CurrentTerm, status.VotedFor, status.CommitIndex)).IsEqualTo((4UL, "node-b", 1UL));
    }

    /// <summary>A snapshot install leaves the state and the offer as they were, also after a reopen.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task StateSurvivesSnapshotInstall(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-state-install");
        using var sourceDir = new TempDirectory("squirix-member-state-install-source");

        await using var source = OpenLog(sourceDir);
        await source.OpenSeededAsync(cancellationToken);
        _ = await source.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
        _ = await source.AdvanceCommitAsync(1UL, cancellationToken);
        var snapshot = await FollowerSnapshotScenario.CompactThroughAsync(source, sourceDir, 1UL, cancellationToken);

        await using (var log = OpenLog(dir))
        {
            await log.OpenSeededAsync(cancellationToken);
            _ = await log.AppendAsync(FollowerFoundationScenario.Append("leader", 1UL, 1UL, "a"), cancellationToken);
            _ = await log.AdvanceCommitAsync(1UL, cancellationToken);
            _ = await log.AdvanceAppliedAsync(1UL, cancellationToken);

            var result = await log.InstallSnapshotAsync(snapshot, 1UL, cancellationToken);

            _ = await Assert.That(result.Success).IsTrue();
            await AssertCompleteAsync(log, cancellationToken);
        }

        await using var reopened = OpenLog(dir);
        await reopened.OpenAsync(cancellationToken);
        await AssertCompleteAsync(reopened, cancellationToken);
    }

    /// <summary>A log opened over a metadata file of an earlier version fails readiness with the earlier-build message and leaves the file unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task EarlierVersionFileFailsReadiness(CancellationToken cancellationToken)
    {
        using var dir = new TempDirectory("squirix-member-state-earlier");
        _ = Directory.CreateDirectory(GroupStoragePaths.GetGroupDirectory(dir, GroupId));
        var path = GroupStoragePaths.GetMetadataPath(dir, GroupId);
        var image = GroupMemberStateCodecTests.EarlierImage();
        await File.WriteAllBytesAsync(path, image, cancellationToken);

        await using var log = OpenLog(dir);
        var ex = await NodeAsyncAssert.ThrowsAsync<InvalidDataException>(log.OpenAsync(cancellationToken));

        _ = await Assert.That(ex.Message).Contains("written by an earlier build and is not supported", StringComparison.Ordinal);
        _ = await Assert.That(log.Readiness).IsEqualTo(FollowerLogReadiness.Failed);
        var after = await File.ReadAllBytesAsync(path, cancellationToken);
        await SequenceAssert.EqualAsync(image, after);
    }

    private static async Task AssertCompleteAsync(FollowerLog log, CancellationToken cancellationToken)
    {
        var status = await log.GetStatusAsync(cancellationToken);
        _ = await Assert.That((status.MemberState, status.BootstrapOffered)).IsEqualTo((GroupMemberState.Complete, false));
    }

    private static FollowerLog OpenLog(TempDirectory dir) => new(dir, GroupId, GroupComposition.Create(GroupId), NullLogger<FollowerLog>.Instance, TimeProvider.System);
}
