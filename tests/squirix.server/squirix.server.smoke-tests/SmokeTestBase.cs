using System;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;

namespace Squirix.Server.SmokeTests;

/// <summary>
/// Base class for all smoke tests, providing helper methods to start test nodes,
/// manage test directories, construct HTTP clients, and build common cache entries.
/// </summary>
public abstract class SmokeTestBase : IDisposable
{
    private static readonly TestNodeSecurityOptions UnauthenticatedSecurity = new();

    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();
    private HttpClient? _httpClient;

    private ClusterIdentity? _identity;

    /// <summary>Gets a reusable <see cref="HttpClient" /> configured for gRPC/HTTP2 smoke testing.</summary>
    protected HttpClient HttpClient => _httpClient ??= CreateHttpClient();

    /// <summary>
    /// Disposes resources allocated by the test base: <see cref="SocketsHttpHandler" />
    /// and <see cref="HttpClient" />.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Reserves one loopback listen URI and starts a single-node cluster.</summary>
    /// <param name="nodeId">Node identifier to start.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal ValueTask<TestCluster<SmokeStartOptions>> StartClusterAsync(
        string nodeId,
        Func<string, SmokeStartOptions>? factory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync([new ClusterNode(nodeId, GetNextHttpUri())], factory, cancellationToken);

    /// <summary>Reserves one loopback listen URI per node and starts a two-node cluster.</summary>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="optionsFactory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal ValueTask<TestCluster<SmokeStartOptions>> StartClusterAsync(
        string nodeA,
        string nodeB,
        Func<string, SmokeStartOptions>? optionsFactory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync(
        [new ClusterNode(nodeA, GetNextHttpUri()), new ClusterNode(nodeB, GetNextHttpUri())],
        optionsFactory,
        cancellationToken);

    /// <summary>Starts a single-node cluster for the supplied topology entry.</summary>
    /// <param name="node">Node identifier paired with its listen URI.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal ValueTask<TestCluster<SmokeStartOptions>> StartClusterAsync(
        ClusterNode node,
        Func<string, SmokeStartOptions>? factory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync([node], factory, cancellationToken);

    /// <summary>Creates a gRPC channel configured for HTTPS against a test node URL.</summary>
    /// <param name="uri">The node listen URL.</param>
    /// <returns>A disposable gRPC channel.</returns>
    protected static GrpcChannel CreateGrpcChannel(Uri uri) => GrpcChannel.ForAddress(
        uri,
        new GrpcChannelOptions
        {
            HttpHandler = LoopbackHttp.CreateHandler(),
            MaxReceiveMessageSize = EntryLimits.GrpcMaxReceiveMessageSizeBytes,
            MaxSendMessageSize = EntryLimits.GrpcMaxSendMessageSizeBytes,
        });

    /// <summary>Disposes managed resources owned by the test base.</summary>
    /// <param name="disposing">True when called from <see cref="Dispose()" />; false from a finalizer path.</param>
    [UsedImplicitly]
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
            return;

        _identity?.Dispose();
        _socketsHttpHandler.Dispose();
        _httpClient?.Dispose();
    }

    /// <summary>Convenience builder for a <see cref="NodeCacheEntry{T}" /> with optional expiration, version, and tags.</summary>
    /// <param name="value">
    /// The value to store. If a JsonDocument or JsonElement is supplied, it is cloned to detach from the
    /// underlying document's lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">Optional absolute UTC expiration time. When <see langword="null" />, the entry has no absolute expiry.</param>
    /// <param name="version">The initial monotonic version to assign to the entry. Defaults to <c language="csharp">1</c>.</param>
    /// <param name="tags">
    /// Optional set of user-defined tags. When provided, the collection is defensively copied
    /// using an ordinal string comparer to prevent external mutation.
    /// </param>
    /// <returns>
    /// A new <see cref="NodeCacheEntry{T}" /> containing the provided <paramref name="value" />,
    /// <paramref name="expiresUtc" />, <paramref name="version" />, and <paramref name="tags" /> (if any).
    /// The <c language="csharp">Expiration</c> property is set to <see langword="null" />.
    /// </returns>
    private protected static NodeCacheEntry<object?> BuildEntry(object? value, DateTime? expiresUtc = null, long version = 1, IDictionary<string, string>? tags = null)
    {
        var v = value switch
        {
            JsonDocument doc => doc.RootElement.Clone(),
            JsonElement elem => elem.Clone(),
            _ => value,
        };

        return new NodeCacheEntry<object?>(v, version, expiresUtc, null, tags?.ToFrozenDictionary(StringComparer.Ordinal));
    }

    /// <summary>Resolves the cluster-aware cache API client from the node's dependency injection container.</summary>
    /// <param name="host">The started test node host that exposes the service provider.</param>
    /// <returns>The resolved <see cref="ICacheApi{T}" /> instance to interact with the node.</returns>
    /// <exception cref="InvalidOperationException">Thrown if <see cref="ICacheApi{T}" /> is not registered in the node's service provider.</exception>
    private protected static ICacheApi<object?> GetCacheApiClient(ITestNodeHost host) => host.GetRequiredService<ICacheApi<object?>>();

    private static string? FindSelfNodeId(ServerPeer[] peers, Uri uri)
    {
        ArgumentNullException.ThrowIfNull(uri);
        for (var index = 0; index < peers.Length; index++)
        {
            var peer = peers[index];
            if (ListenUris.SameAuthority(peer.Uri, uri))
                return peer.NodeId;
        }

        return null;
    }

    /// <summary>Allocates a unique loopback HTTPS listen URI, held bound until <see cref="StartClusterAsync(System.Uri,Squirix.Server.Cluster.ServerPeer[],Squirix.Server.SmokeTests.SmokeStartOptions?,System.Threading.CancellationToken)" /> releases it for the real bind.</summary>
    /// <returns>A loopback HTTPS listen URI.</returns>
    private static Uri GetNextHttpUri() => ListenPortPool.SmokeTests.HoldHttpUri();

    /// <summary>Fails loudly on <see cref="ClusterStartOptions" /> members this starter does not wire into node startup.</summary>
    /// <param name="options">The options to validate.</param>
    /// <exception cref="NotSupportedException">Thrown when an unsupported member is set to a non-default value.</exception>
    private static void ThrowIfUnsupportedClusterStartOptions(SmokeStartOptions options)
    {
        if (options.DataDir != null || options.MtlsProfile != TestNodeProfile.Normal || options.TimeProvider != null || options.ReplicaCount != 1 || !options.EnableReplication ||
            options.ConfigurationGeneration != 1)
        {
            throw new NotSupportedException(
                "SmokeStartOptions does not wire DataDir, MtlsProfile, TimeProvider, ReplicaCount, EnableReplication, or ConfigurationGeneration into node startup.");
        }
    }

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };

    /// <summary>Starts one node per topology entry with a shared peer set.</summary>
    /// <param name="topology">Node identifiers paired with their listen URIs, in start order.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    private async ValueTask<TestCluster<SmokeStartOptions>> StartClusterAsync(
        ClusterNode[] topology,
        Func<string, SmokeStartOptions>? factory = null,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var peers = ClusterIdentity.CreatePeers(topology, ref _identity);
        TestCluster<SmokeStartOptions>? cluster = null;
        try
        {
            cluster = TestCluster<SmokeStartOptions>.Create(
                topology,
                (self, nodeTopology, options, token) => StartClusterAsync(self.Uri, ClusterIdentity.CreatePeers(nodeTopology, ref _identity), options, token),
                peers);

            var started = await cluster.StartAllAsync(factory, i => ListenPortPool.SmokeTests.ReleasePort(topology[i].Uri.Port), cancellationToken).ConfigureAwait(false);
            cluster = null;
            return started;
        }
        finally
        {
            if (cluster != null)
                await cluster.DisposeAsync().ConfigureAwait(false);
        }
    }

    private async ValueTask<ITestNodeHost> StartClusterAsync(Uri uri, ServerPeer[] peers, SmokeStartOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new SmokeStartOptions();
        ThrowIfUnsupportedClusterStartOptions(options);
        ArgumentNullException.ThrowIfNull(uri);
        var canonicalUri = new Uri(ListenUris.CanonicalAuthority(uri), UriKind.Absolute);
        var selfNodeId = FindSelfNodeId(peers, canonicalUri) ??
                         ThrowHelper.Throw<string>(new ArgumentException("The peers list must contain an entry for the node being started", nameof(peers)));

        var clusterConfig = new TopologyOptions(peers)
        {
            NodeId = selfNodeId,
            Uri = canonicalUri,
            VirtualNodes = 128,
        };

        try
        {
            (_identity, var mtlsOptions, var mtlsMaterial) = await ClusterIdentity.ResolveForBindAsync(_identity, clusterConfig, cancellationToken).ConfigureAwait(false);
            ListenPortPool.SmokeTests.ReleasePort(canonicalUri.Port);
            var app = await NodeHost.StartAsync(
                clusterConfig,
                new NodeHostStartOptions
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
                    BackpressureOptions = options.BackpressureOptions,
                    MemoryPressureOptions = options.MemoryPressureOptions,
                    SecurityOptions = (options.Security ?? UnauthenticatedSecurity).ToServerOptions(),
                    MtlsOptions = mtlsOptions,
                    Certificate = mtlsMaterial,
                },
                cancellationToken);

            return new TestNodeHost(app, canonicalUri, string.Empty);
        }
        catch
        {
            ListenPortPool.SmokeTests.ReleasePort(canonicalUri.Port);
            throw;
        }
    }
}
