using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Runtime;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.Mtls;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Starts in-process Squirix nodes for external black-box tests.</summary>
public static class TestNodeHostFactory
{
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
        Uri uri,
        ReadOnlySpan<(string NodeId, Uri Uri)> topology,
        TestNodeHostStartOptions? options,
        ClusterTls? sharedMtls,
        CancellationToken cancellationToken = default)
    {
        return StartNodeAsync(nodeId, uri, CopyTopology(topology), options, sharedMtls, cancellationToken);
    }

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

            persistenceOptions = BuildPersistenceOptions(options!);
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
                SnapshotOptions = BuildSnapshotOptions(options),
                PeerHandlerFactory = peerHandlerFactory,
                SecurityOptions = options?.Security?.ToServerOptions(),
                MtlsOptions = mtlsOptions,
                MtlsMaterial = mtlsMaterial,
            },
            cancellationToken).ConfigureAwait(false);

        return new TestNodeHost(app, uri, persistenceOptions?.DataDir ?? string.Empty, persistenceOptions is not null);
    }

    private static PersistenceOptions BuildPersistenceOptions(TestNodeHostStartOptions hostOptions)
    {
        var options = new PersistenceOptions { DataDir = hostOptions.DataDir ?? string.Empty };
        if (hostOptions.JournalMaxSegmentMb is { } segmentMb)
            options = options with { JournalMaxSegmentMb = segmentMb };

        if (hostOptions.JournalMaxSegmentCount is { } segmentCount)
            options = options with { JournalMaxSegmentCount = segmentCount };

        if (hostOptions.JournalMaxTotalBytesMb is { } totalBytesMb)
            options = options with { JournalMaxTotalBytesMb = totalBytesMb };

        if (hostOptions.FlushIntervalMs is { } flushIntervalMs)
            options = options with { FlushIntervalMs = flushIntervalMs };

        if (hostOptions.JournalGroupCommitMaxWaitMs is { } groupCommitMaxWaitMs)
            options = options with { JournalGroupCommitMaxWait = TimeSpan.FromMilliseconds(groupCommitMaxWaitMs) };

        return options;
    }

    private static TriggerOptions? BuildSnapshotOptions(TestNodeHostStartOptions? hostOptions)
    {
        if (hostOptions?.SnapshotInterval is not { } snapshotInterval)
            return null;

        var interval = snapshotInterval.ToString("c", System.Globalization.CultureInfo.InvariantCulture);
        return new ServerJsonSerializer().Deserialize<TriggerOptions>($$"""{"snapshotInterval":"{{interval}}"}""");
    }
}
