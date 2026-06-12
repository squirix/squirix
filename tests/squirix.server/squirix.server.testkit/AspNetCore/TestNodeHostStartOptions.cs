using Squirix.Server.TestKit.Cluster;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Optional settings for <see cref="TestNodeHostFactory" /> node startup.
/// </summary>
public sealed class TestNodeHostStartOptions
{
    /// <summary>
    /// Gets the persistence data directory. When set, the node starts with WAL/snapshot persistence enabled.
    /// </summary>
    public string? DataDir { get; init; }

    /// <summary>
    /// Gets shared cluster mTLS context for multi-node topologies in the same test case.
    /// </summary>
    public MtlsTestContext? Mtls { get; init; }

    /// <summary>
    /// Gets the inter-node mTLS profile for this node in negative-path cluster tests.
    /// </summary>
    public MtlsTestNodeProfile MtlsProfile { get; init; } = MtlsTestNodeProfile.Normal;

    /// <summary>
    /// Gets optional per-node security settings.
    /// </summary>
    public TestNodeSecurityOptions? Security { get; init; }
}
