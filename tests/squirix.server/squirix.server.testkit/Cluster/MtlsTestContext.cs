using System;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.
/// </summary>
public sealed class MtlsTestContext : IDisposable
{
    private MtlsTestBundle? _bundle;

    /// <inheritdoc />
    public void Dispose()
    {
        _bundle?.Dispose();
        _bundle = null;
    }

    /// <summary>
    /// Resolves startup overrides for a node, reusing one shared context per test case.
    /// </summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Startup overrides, or <c>null</c> values for standalone topology.</returns>
    internal static (MtlsOptions? Options, MtlsCertificateMaterial? Material) ResolveForNode(
        ref MtlsTestContext? shared,
        ClusterConfig cluster,
        string url)
    {
        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null);

        shared ??= new MtlsTestContext();
        return shared.Resolve(cluster, url);
    }

    /// <summary>
    /// Creates cluster mTLS startup overrides for the node being started.
    /// </summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    internal (MtlsOptions? Options, MtlsCertificateMaterial? Material) Resolve(ClusterConfig cluster, string url)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(url);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null);

        _bundle ??= new MtlsTestBundle();
        var primaryPort = new Uri(url).Port;
        var internalPort = MtlsTestPorts.AllocateInternalPort(primaryPort);
        return _bundle.CreateNode(cluster.NodeId, primaryPort, internalPort);
    }
}
