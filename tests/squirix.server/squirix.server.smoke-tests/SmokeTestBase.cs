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
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using Xunit;

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

    private ClusterTls? _mtls;

    /// <summary>Gets a default cancellation token with a fixed timeout (~30s) for smoke tests.</summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;

    /// <summary>
    /// Gets a reusable <see cref="HttpClient" /> configured for gRPC/HTTP2 smoke testing.
    /// </summary>
    protected HttpClient HttpClient => _httpClient ??= CreateHttpClient();

    /// <summary>
    /// Disposes resources allocated by the test base: <see cref="SocketsHttpHandler" />,
    /// <see cref="HttpClient" />, and default <see cref="CancellationTokenSource" />.
    /// </summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Builds cluster peer entries, provisioning inter-node mTLS URLs for multi-node topologies.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    internal ServerPeer[] BuildClusterPeers(ReadOnlySpan<(string NodeId, Uri Uri)> topology) => ClusterTls.CreatePeers(topology, ref _mtls);

    internal ValueTask<TestNodeHost> StartNodeAsync(string uri, string nodeId, SmokeNodeStartOptions? options = null, CancellationToken cancellationToken = default) =>
        StartNodeAsync(uri, BuildClusterPeer(nodeId, new Uri(uri, UriKind.Absolute)), options, cancellationToken);

    internal ValueTask<TestNodeHost> StartNodeAsync(Uri uri, string nodeId, SmokeNodeStartOptions? options = null, CancellationToken cancellationToken = default) =>
        StartNodeAsync(uri, BuildClusterPeer(nodeId, uri), options, cancellationToken);

    internal async ValueTask<TestNodeHost> StartNodeAsync(Uri uri, ServerPeer[] peers, SmokeNodeStartOptions? options = null, CancellationToken cancellationToken = default)
    {
        options ??= new SmokeNodeStartOptions();
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

        (_mtls, var mtlsOptions, var mtlsMaterial) = await ClusterTls.ResolveForNodeAsync(_mtls, clusterConfig, canonicalUri, cancellationToken).ConfigureAwait(false);
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
                MtlsMaterial = mtlsMaterial,
            },
            cancellationToken);

        return new TestNodeHost(app, canonicalUri, string.Empty);
    }

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

    /// <summary>
    /// Gets listen URLs for a node bound on all interfaces (<c language="csharp">0.0.0.0</c>) and scraped via loopback.
    /// </summary>
    /// <returns>A tuple of bind URL and loopback scrape URL sharing the same port.</returns>
    protected static (string BindUrl, string LoopbackUrl) GetNextAnyInterfaceListenUrls()
    {
        var port = ListenPortPool.SmokeTests.AllocatePort();
        return (NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", port), NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", port));
    }

    /// <summary>Allocates a unique loopback HTTPS listen URI for the next node using the shared port pool.</summary>
    /// <returns>A loopback HTTPS listen URI.</returns>
    protected static Uri GetNextHttpUri() => ListenPortPool.SmokeTests.NextHttpUri();

    /// <summary>Disposes managed resources owned by the test base.</summary>
    /// <param name="disposing">True when called from <see cref="Dispose()" />; false from a finalizer path.</param>
    [UsedImplicitly]
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
            return;

        _mtls?.Dispose();
        _socketsHttpHandler.Dispose();
        _httpClient?.Dispose();
    }

    /// <summary>
    /// Convenience builder for a <see cref="NodeCacheEntry{T}" /> with optional expiration, version, and tags.
    /// </summary>
    /// <param name="value">
    /// The value to store. If a JsonDocument or JsonElement is supplied, it is cloned to detach from the
    /// underlying document's lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">
    /// Optional absolute UTC expiration time. When <see langword="null" />, the entry has no absolute expiry.
    /// </param>
    /// <param name="version">
    /// The initial monotonic version to assign to the entry. Defaults to <c language="csharp">1</c>.
    /// </param>
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
    /// <returns>
    /// The resolved <see cref="ICacheApi{T}" /> instance to interact with the node.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <see cref="ICacheApi{T}" /> is not registered in the node's service provider.
    /// </exception>
    private protected static ICacheApi<object?> GetCacheApiClient(TestNodeHost host) => host.Services.GetRequiredService<ICacheApi<object?>>();

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

    /// <summary>Builds a standalone single-peer topology without a temporary one-element collection.</summary>
    /// <param name="nodeId">Local node identifier.</param>
    /// <param name="uri">Primary listen URL.</param>
    /// <returns>A one-element peer array.</returns>
    private ServerPeer[] BuildClusterPeer(string nodeId, Uri uri) => ClusterTls.CreatePeer(nodeId, uri, ref _mtls);

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };

    /// <summary>
    /// Starts a new <see cref="NodeHost" /> instance configured for testing,
    /// using the provided peers and service options.
    /// </summary>
    /// <param name="uri">The URL this node should bind to.</param>
    /// <param name="peers">Cluster peers including this node.</param>
    /// <param name="options">Optional startup knobs (security, gRPC, services, etc.).</param>
    /// <param name="cancellationToken">Cancellation token to stop startup.</param>
    /// <returns>A started <see cref="TestNodeHost" /> wrapper around the node.</returns>
    private ValueTask<TestNodeHost> StartNodeAsync(string uri, ServerPeer[] peers, SmokeNodeStartOptions? options = null, CancellationToken cancellationToken = default) =>
        StartNodeAsync(new Uri(uri, UriKind.Absolute), peers, options, cancellationToken);
}
