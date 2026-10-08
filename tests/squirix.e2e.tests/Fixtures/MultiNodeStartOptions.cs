using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Squirix.Attributes;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Optional startup settings for multi-node E2E clusters.</summary>
[Immutable]
internal sealed class MultiNodeStartOptions
{
    /// <summary>Initializes the internode mTLS profile for node A.</summary>
    internal TestNodeProfile NodeAProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Initializes the internode mTLS profile for node B.</summary>
    internal TestNodeProfile NodeBProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Initializes the internode mTLS profile for node C.</summary>
    internal TestNodeProfile NodeCProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Initializes the internode mTLS profile for node D.</summary>
    internal TestNodeProfile NodeDProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Initializes the internode mTLS profile for node E.</summary>
    internal TestNodeProfile NodeEProfile { private get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the election timing applied to every node; null keeps the product defaults.</summary>
    internal TestElectionTiming? ElectionTiming { get; init; }

    /// <summary>
    /// Gets a value indicating whether every node runs automatic failover with quorum reads: groups of three or more replicas elect their
    /// leader and serve reads only with majority confirmation.
    /// </summary>
    /// <remarks>Elections run on the node clock, so a cluster that combines this switch with a <see cref="FakeTimeProvider" /> fails to start.</remarks>
    internal bool Failover { get; init; }

    /// <summary>Gets an optional per-node clock, receiving the node identifier; a null result falls back to <see cref="TimeProvider" />.</summary>
    internal Func<string, TimeProvider?>? NodeClock { get; init; }

    /// <summary>Gets the fabric every internode connection is dialed through, so tests cut and heal links; null dials peers directly.</summary>
    /// <remarks>The fabric must outlive the cluster.</remarks>
    internal PartitionFabric? PartitionFabric { get; init; }

    /// <summary>Gets the replica factor applied to every node; 1 preserves single-copy routing.</summary>
    internal int ReplicaCount { get; init; } = 1;

    /// <summary>Gets optional external auth settings applied to both nodes.</summary>
    internal TestNodeSecurityOptions? Security { get; init; }

    /// <summary>Gets an optional per-node hook that registers additional services, receiving the node identifier.</summary>
    internal Action<string, IServiceCollection>? ServicesConfigure { get; init; }

    /// <summary>Gets the shared node time source applied to every node; null keeps the real system clock.</summary>
    internal TimeProvider? TimeProvider { get; init; }

    /// <summary>Gets the clock of one node: its own clock when <see cref="NodeClock" /> gives one, otherwise the shared clock.</summary>
    /// <param name="nodeId">Node identifier.</param>
    /// <returns>The node clock; null keeps the real system clock.</returns>
    internal TimeProvider? ClockFor(string nodeId) => NodeClock?.Invoke(nodeId) ?? TimeProvider;

    /// <summary>Rejects a failover cluster whose node runs on a fake clock, which would freeze its elections.</summary>
    /// <param name="nodeIds">The nodes of the cluster.</param>
    /// <exception cref="InvalidOperationException"><see cref="Failover" /> is on and a node clock is a <see cref="FakeTimeProvider" />.</exception>
    internal void EnsureElectionClocks(ReadOnlySpan<string> nodeIds)
    {
        if (!Failover)
            return;

        foreach (var nodeId in nodeIds)
        {
            if (ClockFor(nodeId) is FakeTimeProvider)
                throw new InvalidOperationException($"Node {nodeId} runs automatic failover on a fake clock, which freezes its elections; use a system or skewed clock.");
        }
    }

    internal TestNodeProfile GetProfile(string nodeId) => nodeId switch
    {
        "nodeA" => NodeAProfile,
        "nodeB" => NodeBProfile,
        "nodeC" => NodeCProfile,
        "nodeD" => NodeDProfile,
        "nodeE" => NodeEProfile,
        _ => throw new ArgumentOutOfRangeException(nameof(nodeId), nodeId, "Unsupported E2E node identifier."),
    };
}
