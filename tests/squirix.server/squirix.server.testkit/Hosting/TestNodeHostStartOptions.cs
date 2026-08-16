using Squirix.Attributes;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Optional settings for <see cref="TestNodeHostFactory" /> node startup.
/// </summary>
[Immutable]
public sealed class TestNodeHostStartOptions
{
    /// <summary>Gets the stopped-topology configuration generation.</summary>
    public ulong ConfigurationGeneration { get; init; } = 1;

    /// <summary>Gets the persistence data directory. When set, the node starts with journal/snapshot persistence enabled.</summary>
    public string? DataDir { get; init; }

    /// <summary>Gets a value indicating whether the closed replication gRPC service is mapped for transport/identity tests without enabling RF&gt;1 mutations.</summary>
    public bool FoundationOnly { get; init; }

    /// <summary>Gets the inter-node mTLS profile for this node in negative-path cluster tests.</summary>
    public TestNodeProfile MtlsProfile { get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the replica factor including the original owner.</summary>
    public int ReplicaCount { get; init; } = 1;

    /// <summary>Gets optional per-node security settings.</summary>
    public TestNodeSecurityOptions? Security { get; init; }
}
