using System;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Optional base settings for in-process node startup, extended by per-project option types.</summary>
[Immutable]
public class ClusterStartOptions
{
    /// <summary>Gets the stopped-topology configuration generation.</summary>
    public ulong ConfigurationGeneration { get; init; } = 1;

    /// <summary>Gets the persistence data directory. When set, the node starts with journal/snapshot persistence enabled.</summary>
    public string? DataDir { get; init; }

    /// <summary>Gets a value indicating whether the node opts into RF&gt;1 replication. Defaults to <see langword="true" /> so existing multi-node tests keep exercising replication; opt-in gate tests set it to <see langword="false" /> explicitly.</summary>
    public bool EnableReplication { get; init; } = true;

    /// <summary>Gets the internode mTLS profile for this node in negative-path cluster tests.</summary>
    public TestNodeProfile MtlsProfile { get; init; } = TestNodeProfile.Normal;

    /// <summary>
    /// Gets the fabric the node's internode connections are dialed through. When set, every connection this node opens to a
    /// remote peer goes through the fabric's proxy for the pair, so tests cut and heal links by node identifier; when
    /// <see langword="null" />, peers are dialed directly. The fabric must outlive the cluster.
    /// </summary>
    /// <remarks>
    /// The node must use internode mTLS (a topology with remote peers); starting a node with a fabric on any other
    /// path throws <see cref="InvalidOperationException" />. The outbound handlers are owned by the cluster identity,
    /// not by the node, so a stopped node's proxied links stay open until the identity is disposed.
    /// </remarks>
    public PartitionFabric? PartitionFabric { get; init; }

    /// <summary>Gets the replica factor including the original owner.</summary>
    public int ReplicaCount { get; init; } = 1;

    /// <summary>Gets optional per-node security settings.</summary>
    public TestNodeSecurityOptions? Security { get; init; }

    /// <summary>Gets an optional hook that registers additional services on the node after the server composition, for test probes.</summary>
    public Action<IServiceCollection>? ServicesConfigure { get; init; }

    /// <summary>
    /// Gets the node time source. When set, cache expiration (and every subsystem resolving
    /// <see cref="TimeProvider" /> from DI) reads this clock instead of the system time, letting tests
    /// advance time deterministically; when null, the real system clock is used.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}
