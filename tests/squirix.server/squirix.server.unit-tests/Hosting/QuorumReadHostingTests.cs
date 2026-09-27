using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Core;
using Squirix.Server.Errors;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Quorum reads stay disabled on a hosted cluster: reads are served locally without consulting a majority.</summary>
[Immutable]
public sealed class QuorumReadHostingTests : ServerUnitTestBase
{
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
        // expire after the local append, surfacing an ambiguous outcome (CommitOutcomeUnknown).
        // Until that appended entry is applied the committer also refuses the next write outright
        // (TooManyRequests, "replica_apply_pending"); that refusal is definite and retryable by contract.
        // Retry the seed under the SAME operation identity: the committer reports the ambiguous outcome so callers
        // stop retrying under a new identity, and a replay of the same identity resolves the retained entry, whereas a
        // fresh identity would collide with it. The read assertions below still verify the test's contract, and a stall
        // past the bound fails loudly instead of hanging.
        var seedOperationId = Guid.NewGuid().ToString();
        var seedEntry = new NodeCacheEntry<object?> { Value = "v" };
        using var seedBound = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        using var seedLinked = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, seedBound.Token);
        while (true)
        {
            try
            {
                await cache.SetEntryAsync(seedOperationId, "quorum-read", key, seedEntry, seedLinked.Token);
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
