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
    /// Gets optional per-node security settings.
    /// </summary>
    public TestNodeSecurityOptions? Security { get; init; }

    /// <summary>
    /// Gets shared cluster mTLS context for multi-node topologies in the same test case.
    /// </summary>
    public MtlsTestContext? Mtls { get; init; }
}
