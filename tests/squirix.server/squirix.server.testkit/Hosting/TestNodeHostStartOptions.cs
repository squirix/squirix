using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Optional settings for <see cref="TestNodeHostFactory" /> node startup.
/// </summary>
public sealed class TestNodeHostStartOptions
{
    /// <summary>Gets the persistence data directory. When set, the node starts with journal/snapshot persistence enabled.</summary>
    public string? DataDir { get; init; }

    /// <summary>Gets the inter-node mTLS profile for this node in negative-path cluster tests.</summary>
    public TestNodeProfile MtlsProfile { get; init; } = TestNodeProfile.Normal;

    /// <summary>Gets optional per-node security settings.</summary>
    public TestNodeSecurityOptions? Security { get; init; }
}
