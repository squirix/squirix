using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>
/// Starts in-process nodes for black-box tests that must not reach server internals themselves.
/// Server-internal knobs arrive only through <see cref="BlackBoxStartOptions" />; nodes log at debug level,
/// and a node without a security override starts unauthenticated instead of reading process-wide environment variables.
/// </summary>
internal static class BlackBoxCluster
{
    private static readonly TestNodeSecurityOptions UnauthenticatedSecurity = new();

    /// <summary>Starts one node per topology entry with a shared peer set.</summary>
    /// <param name="topology">Node identifiers paired with their held listen URIs, in start order.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes and their shared mTLS identity.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is <see langword="null" />.</exception>
    internal static async ValueTask<TestCluster<BlackBoxStartOptions>> StartAsync(
        ClusterNode[] topology,
        Func<string, BlackBoxStartOptions>? factory = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topology);
        TestCluster<BlackBoxStartOptions>? cluster = null;
        try
        {
            cluster = CreateCluster(topology);
            var started = await cluster.StartAllAsync(factory, i => ListenPortPool.ReleaseHeldPrimary(topology[i].Uri), cancellationToken).ConfigureAwait(false);
            cluster = null;
            return started;
        }
        finally
        {
            if (cluster != null)
                await cluster.DisposeAsync().ConfigureAwait(false);
        }
    }

    private static TestCluster<BlackBoxStartOptions> CreateCluster(ClusterNode[] topology)
    {
        var sharedIdentity = ClusterIdentity.ResolveForTopology(topology, null);
        var peers = ClusterIdentity.CreatePeers(topology, ref sharedIdentity);
        return TestCluster<BlackBoxStartOptions>.Create(
            topology,
            (self, nodeTopology, options, token) => StartNodeAsync(self, nodeTopology, options, sharedIdentity, token),
            peers,
            sharedIdentity);
    }

    private static NodeHostStartOptions CreateNodeHostOptions(BlackBoxStartOptions options, MtlsOptions? mtlsOptions, MtlsCertificate? mtlsMaterial) => new()
    {
        ConfigureLogging = static b =>
        {
            _ = b.ClearProviders();
            _ = b.SetMinimumLevel(LogLevel.Debug);
            _ = b.AddFilter("Grpc", LogLevel.Debug);
            _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Debug);
            _ = b.AddFilter("Squirix", LogLevel.Debug);
            _ = b.AddConsole().AddDebug();
        },
        ConfigureGrpc = options.ConfigureGrpc,
        ServicesConfigure = options.ServicesConfigure,
        BackpressureOptions = options.BackpressureOptions?.ToServerOptions(),
        MemoryPressureOptions = options.MemoryPressureOptions?.ToServerOptions(),
        SecurityOptions = (options.Security ?? UnauthenticatedSecurity).ToServerOptions(),
        MtlsOptions = mtlsOptions,
        Certificate = mtlsMaterial,
    };

    private static string? FindSelfNodeId(ServerPeer[] peers, Uri uri)
    {
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            if (ListenUris.SameAuthority(peer.Uri, uri))
                return peer.NodeId;
        }

        return null;
    }

    private static async ValueTask<ITestNodeHost> StartNodeAsync(
        ClusterNode self,
        ClusterNode[] topology,
        BlackBoxStartOptions? options,
        ClusterIdentity? identity,
        CancellationToken cancellationToken)
    {
        options ??= new BlackBoxStartOptions();
        ThrowIfUnsupportedClusterStartOptions(options);
        var canonicalUri = new Uri(ListenUris.CanonicalAuthority(self.Uri), UriKind.Absolute);

        // The cluster owns a shared identity; an identity created here for this node alone is released with the node.
        var callerOwnedIdentity = identity != null;
        try
        {
            var peers = ClusterIdentity.CreatePeers(topology, ref identity);
            var selfNodeId = FindSelfNodeId(peers, canonicalUri) ??
                             ThrowHelper.Throw<string>(new ArgumentException("The topology must contain an entry for the node being started.", nameof(self)));

            var clusterConfig = new TopologyOptions(peers)
            {
                NodeId = selfNodeId,
                Uri = canonicalUri,
                VirtualNodes = 128,
            };

            var (mtlsOptions, mtlsMaterial, _) = identity == null ? new NodeMtlsStartup(null, null, null)
                : await identity.ResolveNodeStartupForBindAsync(clusterConfig, TestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
            ListenPortPool.ReleaseHeldPrimary(canonicalUri);
            var app = await NodeHost.StartAsync(clusterConfig, CreateNodeHostOptions(options, mtlsOptions, mtlsMaterial), cancellationToken).ConfigureAwait(false);

            return new TestNodeHost(app, canonicalUri, string.Empty, false, callerOwnedIdentity ? null : identity);
        }
        catch
        {
            if (!callerOwnedIdentity)
                identity?.Dispose();

            ListenPortPool.ReleaseHeldPrimary(canonicalUri);
            throw;
        }
    }

    /// <summary>Fails loudly on <see cref="ClusterStartOptions" /> members this starter does not wire into node startup.</summary>
    /// <param name="options">The options to validate.</param>
    /// <exception cref="NotSupportedException">Thrown when an unsupported member is set to a non-default value.</exception>
    private static void ThrowIfUnsupportedClusterStartOptions(BlackBoxStartOptions options)
    {
        if (options.DataDir != null || options.MtlsProfile != TestNodeProfile.Normal || options.TimeProvider != null || options.ReplicaCount != 1 || !options.EnableReplication ||
            options.ConfigurationGeneration != 1)
        {
            throw new NotSupportedException(
                "BlackBoxStartOptions does not wire DataDir, MtlsProfile, TimeProvider, ReplicaCount, EnableReplication, or ConfigurationGeneration into node startup.");
        }
    }
}
