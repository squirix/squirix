using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Starts in-process Squirix nodes for external black-box tests.</summary>
public static class TestNodeHostFactory
{
    /// <summary>Starts an ephemeral standalone (single-peer) node without a temporary topology collection.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="uri">The HTTP listen address.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(string nodeId, Uri uri, CancellationToken cancellationToken = default) =>
        StartNodeAsync(nodeId, uri, [(nodeId, uri)], null, null, cancellationToken);

    /// <summary>Starts a standalone (single-peer) node with an optional persistence directory.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="uri">The HTTP listen address.</param>
    /// <param name="dataDir">Persistence data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(string nodeId, Uri uri, string? dataDir, CancellationToken cancellationToken = default) => StartNodeAsync(
        nodeId,
        uri,
        [(nodeId, uri)],
        dataDir is null ? null : new TestNodeHostStartOptions { DataDir = dataDir },
        null,
        cancellationToken);

    /// <summary>Starts a test node with shared cluster mTLS material owned by the caller.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="uri">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="options">Optional startup settings.</param>
    /// <param name="sharedMtls">Caller-owned shared mTLS context for multi-node topologies; not disposed by the factory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        ReadOnlySpan<(string NodeId, string Address)> topology,
        TestNodeHostStartOptions? options = null,
        CancellationToken cancellationToken = default) => StartNodeAsync(
        nodeId,
        address,
        CopyTopology(topology),
        options?.DataDir,
        options?.DataDir is not null,
        options?.Security,
        options?.Mtls,
        options?.MtlsProfile ?? MtlsTestNodeProfile.Normal,
        cancellationToken);

    /// <summary>Starts an ephemeral in-memory node with the provided cluster topology.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        Uri address,
        ReadOnlySpan<(string NodeId, Uri Uri)> topology,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, options: null, cancellationToken);

    /// <summary>Starts a node with the provided cluster topology and persistence directory.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="uri">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="dataDir">Persistence data directory.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        Uri uri,
        ReadOnlySpan<(string NodeId, Uri Uri)> topology,
        string? dataDir,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, uri, topology, new TestNodeHostStartOptions { DataDir = dataDir }, cancellationToken);

    private static (string NodeId, Uri Uri)[] CopyTopology(ReadOnlySpan<(string NodeId, Uri Uri)> topology)
    {
        var copy = new (string NodeId, Uri Uri)[topology.Length];
        for (var i = 0; i < topology.Length; i++)
            copy[i] = (topology[i].NodeId, topology[i].Uri);

        return copy;
    }

    /// <summary>Starts a test node with the provided cluster topology and optional settings.</summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="uri">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="options">Optional startup settings.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    private static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        Uri uri,
        ReadOnlySpan<(string NodeId, Uri Uri)> topology,
        TestNodeHostStartOptions? options = null,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, uri, topology, options, null, cancellationToken);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The node host client pool owns the handler for the process lifetime of the test node.")]
    private static async ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        Uri uri,
        (string NodeId, Uri Uri)[] topology,
        TestNodeHostStartOptions? options,
        ClusterTls? sharedMtls,
        CancellationToken cancellationToken)
    {
        PersistenceOptions? persistenceOptions = null;
        var dataDir = options?.DataDir;
        if (dataDir is not null)
        {
            if (string.IsNullOrWhiteSpace(dataDir))
                throw new ArgumentException("DataDir must be non-empty when persistence is enabled.", nameof(options));

            persistenceOptions = new PersistenceOptions { DataDir = dataDir };
        }

        var peers = ClusterTls.CreatePeers(ref sharedMtls, topology);

        var clusterConfig = new TopologyOptions(peers)
        {
            NodeId = nodeId,
            Uri = uri,
            VirtualNodes = 128,
        };

        var primaryUri = clusterConfig.Uri;
        var mtlsProfile = options?.MtlsProfile ?? TestNodeProfile.Normal;
        var (mtlsOptions, mtlsMaterial, peerHandlerFactory) = sharedMtls is null ? (null, null, null)
            : await sharedMtls.ResolveNodeStartupAsync(clusterConfig, primaryUri, mtlsProfile, cancellationToken).ConfigureAwait(false);

        var app = await NodeHost.StartAsync(
            clusterConfig,
            new NodeHostStartOptions
            {
                ConfigureLogging = static b =>
                {
                    _ = b.ClearProviders();
                    _ = b.SetMinimumLevel(LogLevel.Warning);
                    _ = b.AddFilter("Grpc", LogLevel.Warning);
                    _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Warning);
                    _ = b.AddFilter("Squirix", LogLevel.Warning);
                },
                PersistenceOptions = persistenceOptions,
                PeerHandlerFactory = peerHandlerFactory,
                SecurityOptions = options?.Security?.ToServerOptions(),
                MtlsOptions = mtlsOptions,
                MtlsMaterial = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);

        return new TestNodeHost(app, uri, persistenceOptions?.DataDir ?? string.Empty, persistenceOptions is not null);
    }

    private static (string NodeId, string Address)[] CopyTopology(ReadOnlySpan<(string NodeId, string Address)> topology)
    {
        var copy = new (string NodeId, string Address)[topology.Length];
        topology.CopyTo(copy);
        return copy;
    }
}
