using System;
using Squirix.Server.Attributes;
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

    /// <summary>Gets a value indicating whether the node opts into RF&gt;1 replication. Defaults to <see langword="true" /> so existing multi-node tests keep exercising replication; opt-in gate tests set it to <see langword="false" /> explicitly.</summary>
    public bool EnableReplication { get; init; } = true;

    /// <summary>Gets the inter-node mTLS profile for this node in negative-path cluster tests.</summary>
    public TestNodeProfile MtlsProfile { get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets the replica factor including the original owner.</summary>
    public int ReplicaCount { get; init; } = 1;

    /// <summary>Gets optional per-node security settings.</summary>
    public TestNodeSecurityOptions? Security { get; init; }

    /// <summary>
    /// Gets the node time source. When set, cache expiration (and every subsystem resolving
    /// <see cref="TimeProvider" /> from DI) reads this clock instead of the system time, letting tests
    /// advance time deterministically; when null, the real system clock is used.
    /// </summary>
    public TimeProvider? TimeProvider { get; init; }
}
