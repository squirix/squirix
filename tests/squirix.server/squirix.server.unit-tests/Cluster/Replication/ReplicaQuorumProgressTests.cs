using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>The progress signal of <see cref="ReplicaCommitQuorum" /> that wakes a commit waiting for a majority.</summary>
[Immutable]
public sealed class ReplicaQuorumProgressTests
{
    /// <summary>A wait on a version that already moved returns at once.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task MovedVersionReturnsAtOnce(CancellationToken cancellationToken)
    {
        var quorum = new ReplicaCommitQuorum(3);
        var seen = quorum.ProgressVersion;
        _ = quorum.TryRecord(1, CreateAcknowledgement(Mutation(1)), Mutation(1));

        var wait = quorum.WaitForProgressAsync(seen, cancellationToken);

        _ = await Assert.That(wait.IsCompletedSuccessfully).IsTrue();
        _ = await Assert.That(quorum.ProgressVersion > seen).IsTrue();
    }

    /// <summary>An acknowledgement buffered behind a missing prefix does not signal, and the one that completes the prefix does.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OnlyContiguousAckSignals(CancellationToken cancellationToken)
    {
        var quorum = new ReplicaCommitQuorum(3);
        var wait = quorum.WaitForProgressAsync(quorum.ProgressVersion, cancellationToken);

        _ = quorum.TryRecord(1, CreateAcknowledgement(Mutation(2)), Mutation(2));
        _ = await Assert.That(wait.IsCompleted).IsFalse();
        _ = await Assert.That(quorum.HasBufferedThrough(2)).IsTrue();
        _ = await Assert.That(quorum.HasBufferedThrough(1)).IsFalse();

        _ = quorum.TryRecord(1, CreateAcknowledgement(Mutation(1)), Mutation(1));
        await wait.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(2UL);
        _ = await Assert.That(quorum.HasBufferedThrough(2)).IsFalse();
    }

    /// <summary>A slot buffers at most the capped number of acknowledgements ahead of its missing prefix; the next one is not recorded.</summary>
    [Test]
    public async Task BufferedAcknowledgementsAreCapped()
    {
        var quorum = new ReplicaCommitQuorum(3);
        for (var index = 2UL; index < 2UL + ReplicaCommitQuorum.MaxBufferedAcks; index++)
            _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(Mutation(index)), Mutation(index))).IsTrue();

        const ulong past = 2UL + ReplicaCommitQuorum.MaxBufferedAcks;
        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(Mutation(past)), Mutation(past))).IsFalse();

        _ = await Assert.That(quorum.TryRecord(1, CreateAcknowledgement(Mutation(1)), Mutation(1))).IsTrue();
        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(past - 1UL);
    }

    /// <summary>Admitting a slot at a higher match index signals; admitting at a lower one does not.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AdmitRaisingMatchSignals(CancellationToken cancellationToken)
    {
        var quorum = new ReplicaCommitQuorum(3, 2);
        var wait = quorum.WaitForProgressAsync(quorum.ProgressVersion, cancellationToken);

        quorum.Admit(1, 1);
        _ = await Assert.That(wait.IsCompleted).IsFalse();

        quorum.Admit(1, 5);
        await wait.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);

        _ = await Assert.That(quorum.MatchIndexFor(1)).IsEqualTo(5UL);
    }

    /// <summary>Canceling the token ends a pending wait as canceled.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancellationEndsWait(CancellationToken cancellationToken)
    {
        var quorum = new ReplicaCommitQuorum(3);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var wait = quorum.WaitForProgressAsync(quorum.ProgressVersion, cancellation.Token);

        await cancellation.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(wait);
    }

    private static ReplicaDurableAcknowledgement CreateAcknowledgement(PreparedReplicaMutation mutation) => new(
        mutation.GroupId,
        mutation.Term,
        mutation.LogIndex,
        mutation.OperationFingerprint,
        mutation.PayloadChecksum,
        true,
        true);

    private static PreparedReplicaMutation Mutation(ulong index) => new(
        new ReplicaOperationIdentity("group-a", "client", $"op-{index}", new byte[] { 1 }),
        1,
        index,
        new ReplicaMutationPayload(new byte[] { 2 }, new byte[] { 3 }, 4));
}
