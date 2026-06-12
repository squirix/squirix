using System;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Cluster;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Starts in-process Squirix nodes for external black-box tests.
/// </summary>
public static class TestNodeHostFactory
{
    /// <summary>
    /// Starts a test node with the provided cluster topology and optional settings.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="options">Optional startup settings.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        TestNodeHostStartOptions? options = null,
        CancellationToken cancellationToken = default) => StartNodeAsync(
        nodeId,
        address,
        topology,
        options?.DataDir,
        options?.DataDir is not null,
        options?.Security,
        options?.Mtls,
        options?.MtlsProfile ?? MtlsTestNodeProfile.Normal,
        cancellationToken);

    /// <summary>
    /// Starts an ephemeral in-memory node with the provided cluster topology.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, options: null, cancellationToken);

    /// <summary>
    /// Starts a node with the provided cluster topology and persistence directory.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="dataDir">Persistence data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        string dataDir,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, new TestNodeHostStartOptions { DataDir = dataDir }, cancellationToken);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The node host client pool owns the handler for the process lifetime of the test node.")]
    private static async ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        string? dataDir,
        bool persistence,
        TestNodeSecurityOptions? security,
        MtlsTestContext? mtls,
        MtlsTestNodeProfile mtlsProfile,
        CancellationToken cancellationToken)
    {
        if (persistence)
            ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        var persistenceOptions = persistence ? new PersistenceOptions { DataDir = dataDir ?? string.Empty } : null;
        var peerTopology = topology.Select(static member => (member.NodeId, member.Address)).ToArray();
        var sharedMtls = mtls;
        var peers = MtlsTestContext.CreatePeers(ref sharedMtls, peerTopology);

        var clusterConfig = new ClusterConfig
        {
            NodeId = nodeId,
            Url = address,
            VirtualNodes = 128,
            Peers = peers,
        };

        var (mtlsOptions, mtlsMaterial, peerHandlerFactory) = mtls?.ResolveNodeStartup(clusterConfig, address, mtlsProfile) ?? (null, null, null);

        var app = await SquirixNodeHost.StartAsync(
            clusterConfig,
            static b =>
            {
                _ = b.ClearProviders();
                _ = b.SetMinimumLevel(LogLevel.Warning);
                _ = b.AddFilter("Grpc", LogLevel.Warning);
                _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Warning);
                _ = b.AddFilter("Squirix", LogLevel.Warning);
            },
            persistenceOptionsOverride: persistenceOptions,
            peerHandlerFactory: peerHandlerFactory,
            securityOptionsOverride: security?.ToServerOptions(),
            mtlsOptionsOverride: mtlsOptions,
            mtlsMaterialOverride: mtlsMaterial,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new TestNodeHost(app, address, persistenceOptions?.DataDir ?? string.Empty, persistence);
    }
}
