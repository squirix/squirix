using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Storage;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.TestKit.AspNetCore;

/// <summary>
/// Starts in-process Squirix nodes for external black-box tests.
/// </summary>
public static class TestNodeHostFactory
{
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
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, null, false, null, null, cancellationToken);

    /// <summary>
    /// Starts an ephemeral in-memory node with optional per-node security settings.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="security">Optional security override. When set, environment variables are not read for auth.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        TestNodeSecurityOptions? security,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, null, false, security, null, cancellationToken);

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
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, dataDir, true, null, null, cancellationToken);

    /// <summary>
    /// Starts a node with optional per-node security settings.
    /// </summary>
    /// <param name="nodeId">The node identifier.</param>
    /// <param name="address">The HTTP listen address.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="dataDir">Persistence data directory.</param>
    /// <param name="security">Optional security override. When set, environment variables are not read for auth.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started test node host.</returns>
    public static ValueTask<TestNodeHost> StartNodeAsync(
        string nodeId,
        string address,
        (string NodeId, string Address)[] topology,
        string dataDir,
        TestNodeSecurityOptions? security,
        CancellationToken cancellationToken = default) => StartNodeAsync(nodeId, address, topology, dataDir, true, security, null, cancellationToken);

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
        Action<SquirixServerExtensionOptions>? configureExtensions,
        CancellationToken cancellationToken)
    {
        if (persistence)
            ArgumentException.ThrowIfNullOrWhiteSpace(dataDir);
        var persistenceOptions = persistence ? new PersistenceOptions { DataDir = dataDir ?? string.Empty } : null;
        var peers = new Peer[topology.Length];
        for (var i = 0; i < topology.Length; i++)
            peers[i] = new Peer { NodeId = topology[i].NodeId, Url = topology[i].Address };

        var clusterConfig = new ClusterConfig
        {
            NodeId = nodeId,
            Url = address,
            VirtualNodes = 128,
            Peers = peers,
        };

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
            httpHandlerOverride: LoopbackHttp.CreateHandler(),
            securityOptionsOverride: security?.ToServerOptions(),
            configureExtensions: configureExtensions,
            cancellationToken: cancellationToken).ConfigureAwait(false);

        return new TestNodeHost(app, address, persistenceOptions?.DataDir ?? string.Empty, persistence);
    }
}
