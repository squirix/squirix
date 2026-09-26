using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Replication;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.UnitTests.Architecture;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Replication;

/// <summary>Quorum reads stay disabled: reads are served locally without consulting a majority.</summary>
[Immutable]
public sealed class QuorumReadActivationTests : ServerUnitTestBase
{
    /// <summary>With quorum reads disabled, an RF=3 current read without quorum confirmation is rejected.</summary>
    [Test]
    public async Task DisabledFlagRejectsRfThreeCurrentRead()
    {
        var rejected = LeaderAuthorityGate.CheckRead(3, true, true, 7, 7, new LeaderReadState(false, 9, 9));
        _ = await Assert.That(rejected.Allowed).IsFalse();
        _ = await Assert.That(rejected.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);
    }

    /// <summary>Explicit post-proof opt-in serves quorum reads only after verified quorum and applied index.</summary>
    [Test]
    public async Task QuorumReadsEnableAfterProofMatrix()
    {
        var uri = new Uri("https://localhost:6001");
        var proof = new TopologyOptions(
        [
            new ServerPeer { NodeId = "node-a", Uri = uri },
            new ServerPeer { NodeId = "node-b", Uri = uri },
            new ServerPeer { NodeId = "node-c", Uri = uri },
        ])
        {
            ClusterId = "cluster",
            NodeId = "node-a",
            Uri = uri,
            ReplicaCount = 3,
            AutomaticFailoverEnabled = true,
            QuorumReadsEnabled = true,
        };
        _ = await Assert.That(proof.QuorumReadsEnabled).IsTrue();

        var gated = FailoverActivationGate.CheckQuorumRead(false, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(gated.Allowed).IsFalse();
        _ = await Assert.That(gated.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);

        var single = FailoverActivationGate.CheckQuorumRead(true, 1, false, false, 1, 1, new LeaderReadState(false, 0, 7));
        _ = await Assert.That(single.Allowed).IsTrue();

        var allowed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(allowed.Allowed).IsTrue();

        var unconfirmed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(false, 9, 9));
        _ = await Assert.That(unconfirmed.Allowed).IsFalse();
        _ = await Assert.That(unconfirmed.Denial).IsEqualTo(LeaderAuthorityDenial.QuorumNotConfirmed);

        var lagging = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 6, new LeaderReadState(true, 4, 5));
        _ = await Assert.That(lagging.Allowed).IsFalse();
        _ = await Assert.That(lagging.Denial).IsEqualTo(LeaderAuthorityDenial.ReadIndexNotApplied);

        var minority = FailoverActivationGate.CheckQuorumRead(true, 3, false, true, 6, 6, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(minority.Allowed).IsFalse();
        _ = await Assert.That(minority.Denial).IsEqualTo(LeaderAuthorityDenial.MinorityFenced);

        var deposed = FailoverActivationGate.CheckQuorumRead(true, 3, true, true, 6, 7, new LeaderReadState(true, 9, 9));
        _ = await Assert.That(deposed.Allowed).IsFalse();
        _ = await Assert.That(deposed.Denial).IsEqualTo(LeaderAuthorityDenial.StaleTerm);
    }

    /// <summary>Release hosting exposes no quorum-read or failover switches before the proof matrix.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseHostCannotEnableBeforeProofMatrix(CancellationToken cancellationToken)
    {
        var root = RepositoryPaths.FindRepositoryRoot();
        var productHost = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "AspNetCoreExtensions.cs"), cancellationToken);
        _ = await Assert.That(productHost).DoesNotContain("QuorumRead", StringComparison.Ordinal);
        _ = await Assert.That(productHost).DoesNotContain("AutomaticFailover", StringComparison.Ordinal);

        var optionsType = await File.ReadAllTextAsync(Path.Join(root, "src", "squirix.server", "SquirixServerOptions.cs"), cancellationToken);
        _ = await Assert.That(optionsType).DoesNotContain("QuorumRead", StringComparison.Ordinal);
        _ = await Assert.That(optionsType).DoesNotContain("AutomaticFailover", StringComparison.Ordinal);
    }

    /// <summary>RF=3 reads are served locally even after every follower stops.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <exception cref="InvalidOperationException">The seed write did not commit before its bound.</exception>
    [Test]
    public async Task RfThreeReadsRemainDisabled(CancellationToken cancellationToken)
    {
        await using var cluster = await TestNodeCluster.StartAsync("nodeA", "nodeB", "nodeC", 3, true, cancellationToken);
        var nodeA = cluster["nodeA"];

        var cache = nodeA.GetCache<object?>("quorum-read");
        var key = nodeA.FindKeyOwnedBy("quorum-read", "nodeA");

        // The seed write races leader election: under parallel CI load the fixed commit budget can
        // expire after the local append, surfacing an ambiguous outcome for a fresh operation id.
        // Until that appended entry is applied the committer also refuses the next write outright
        // (TooManyRequests, "replica_apply_pending"); that refusal is definite and retryable by contract.
        // Retry the seed with a fresh identity until it commits; the read assertions below still
        // verify the test's contract, and a stall past the bound fails loudly instead of hanging.
        using var seedBound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var seedLinked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, seedBound.Token);
        while (true)
        {
            try
            {
                await cache.SetEntryAsync(Guid.NewGuid().ToString(), "quorum-read", key, new NodeCacheEntry<object?> { Value = "v" }, seedLinked.Token);
                break;
            }
            catch (SquirixException error) when ((error.Code == SquirixErrorCode.CommitOutcomeUnknown || error.Code == SquirixErrorCode.TooManyRequests) &&
                                                 !cancellationToken.IsCancellationRequested)
            {
                if (seedBound.IsCancellationRequested)
                    throw new InvalidOperationException("Seed write did not reach a majority before the bound.", error);

                // A refused write returns immediately; pause so the retry does not spin and starve the pending apply of CPU.
                await Task.Delay(TimeSpan.FromMilliseconds(20), TimeProvider.System, seedLinked.Token);
            }
        }

        await cluster.StopNodeAsync("nodeC");
        var majorityRead = await cache.GetValueAsync("quorum-read", key, cancellationToken);
        _ = await Assert.That(majorityRead.Found).IsTrue();

        // No majority remains, yet the read is still served locally: no quorum gate is consulted.
        await cluster.StopNodeAsync("nodeB");
        var loneRead = await cache.GetValueAsync("quorum-read", key, cancellationToken);
        _ = await Assert.That(loneRead.Found).IsTrue();
        var loneValue = await Assert.That(loneRead.Value).IsTypeOf<string>();
        _ = await Assert.That(loneValue).IsEqualTo("v");
    }
}
