using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Client;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests.Cluster;

/// <summary>
/// End-to-end coverage that a mutation whose forwarded response is lost between the entry node and the key owner
/// is replayed, not re-executed, when the client fails over to a third node.
/// </summary>
/// <remarks>
/// The entry node (nodeA) forwards to the owner (nodeB); the owner executes, then every forward from the entry node fails as unreachable. The entry
/// node forwards once per client attempt, so the client retries on nodeA with the same operation id and then fails over to nodeC.
/// While the response is lost, a direct write to the owner changes the key as a later writer would; a re-executed mutation would overwrite that write, a replayed one leaves it untouched.
/// </remarks>
public sealed class LostForwardResponseE2ETests : EndToEndTestBase
{
    private const string CacheName = "default";

    /// <summary>GetOrAdd keeps its first outcome and does not add the entry again when the client fails over.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task GetOrAddReplaysAfterLostForwardResponse(CancellationToken cancellationToken)
    {
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeB", "lost-forward-get-or-add");
        await using var scenario = await Scenario.StartAsync(nameof(GetOrAddReplaysAfterLostForwardResponse), key, static async (owner, k, ct) => _ = await owner.RemoveAsync(k, ct), cancellationToken);

        var result = await scenario.Cache.GetOrAddAsync(key, static (_, _) => Task.FromResult<string?>("first"), cancellationToken: cancellationToken);

        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value).IsEqualTo("first");
        await scenario.AssertFailedOverWithSingleExecutionAsync();
        _ = await Assert.That((await scenario.Owner.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    /// <summary>Set keeps its first outcome and does not overwrite a later write when the client fails over.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SetReplaysAfterLostForwardResponse(CancellationToken cancellationToken)
    {
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeB", "lost-forward-set");
        await using var scenario = await Scenario.StartAsync(nameof(SetReplaysAfterLostForwardResponse), key, static (owner, k, ct) => owner.SetAsync(k, "second", cancellationToken: ct), cancellationToken);

        await scenario.Cache.SetAsync(key, "first", cancellationToken: cancellationToken);

        await scenario.AssertFailedOverWithSingleExecutionAsync();
        var read = await scenario.Owner.GetValueAsync(key, cancellationToken);
        _ = await Assert.That(read.Found).IsTrue();
        _ = await Assert.That(read.Value).IsEqualTo("second");
    }

    /// <summary>TryAdd keeps its first outcome and does not add the entry again when the client fails over.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ConditionalAddReplaysAfterLostResponse(CancellationToken cancellationToken)
    {
        var key = KeyOwnerHelper.ThreeNode.FindKeyOwnedBy(CacheName, "nodeB", "lost-forward-try-add");
        await using var scenario = await Scenario.StartAsync(nameof(ConditionalAddReplaysAfterLostResponse), key, static async (owner, k, ct) => _ = await owner.RemoveAsync(k, ct), cancellationToken);

        var added = await scenario.Cache.TryAddAsync(key, "first", cancellationToken: cancellationToken);

        _ = await Assert.That(added).IsTrue();
        await scenario.AssertFailedOverWithSingleExecutionAsync();
        _ = await Assert.That((await scenario.Owner.GetValueAsync(key, cancellationToken)).Found).IsFalse();
    }

    private sealed class Scenario : IAsyncDisposable
    {
        private readonly HostedCluster _cluster;
        private readonly string _key;
        private readonly LostForwardResponseProbe _probe;

        private Scenario(HostedCluster cluster, LostForwardResponseProbe probe, string key, ISquirixClient client, ICache<string> cache, ICache<string> owner)
        {
            _cluster = cluster;
            _probe = probe;
            _key = key;
            Client = client;
            Cache = cache;
            Owner = owner;
        }

        internal ICache<string> Cache { get; }

        internal ISquirixClient Client { get; }

        internal ICache<string> Owner { get; }

        public async ValueTask DisposeAsync()
        {
            await Client.DisposeAsync();
            await _cluster.DisposeAsync();
        }

        /// <summary>Starts the cluster and a client that prefers the faulty entry node nodeA and fails over to nodeC.</summary>
        /// <param name="testName">The label for the cluster.</param>
        /// <param name="key">The key owned by nodeB.</param>
        /// <param name="laterWrite">The direct write to the owner that happens while the response is lost.</param>
        /// <param name="cancellationToken">The test cancellation token.</param>
        /// <returns>The started scenario.</returns>
        internal static async ValueTask<Scenario> StartAsync(string testName, string key, Func<ICache<string>, string, CancellationToken, Task> laterWrite, CancellationToken cancellationToken)
        {
            var probe = new LostForwardResponseProbe();
            var options = new MultiNodeStartOptions { ServicesConfigure = new EntryProbe(probe).Configure };
            var cluster = await HostedCluster.StartThreeNodeAsync(testName, options, cancellationToken: cancellationToken);
            try
            {
                var owner = await cluster.GetCacheAsync<string>(CacheName, "nodeB", cancellationToken);
                probe.Lose(key, () => laterWrite(owner, key, cancellationToken));
                var client = await LoopbackConnect.ConnectAsync(cluster.GetUri("nodeA"), cluster.GetUri("nodeC"), cancellationToken);
                var cache = await client.GetCacheAsync<string>(CacheName, cancellationToken);
                return new Scenario(cluster, probe, key, client, cache, owner);
            }
            catch
            {
                await cluster.DisposeAsync();
                throw;
            }
        }

        /// <summary>Asserts the client retried on the entry node (one forward per client attempt) before failing over, and the owner ran the mutation once.</summary>
        /// <returns>A task that completes when the assertions pass.</returns>
        internal async Task AssertFailedOverWithSingleExecutionAsync()
        {
            _ = await Assert.That(_probe.OwnerExecutions(_key)).IsEqualTo(1);
            _ = await Assert.That(_probe.ForwardAttempts(_key)).IsEqualTo(3);
        }
    }

    private sealed class EntryProbe
    {
        private readonly LostForwardResponseProbe _probe;

        internal EntryProbe(LostForwardResponseProbe probe)
        {
            _probe = probe;
        }

        internal void Configure(string nodeId, IServiceCollection services)
        {
            if (string.Equals(nodeId, "nodeA", StringComparison.Ordinal))
                _probe.Register(services);
        }
    }
}
