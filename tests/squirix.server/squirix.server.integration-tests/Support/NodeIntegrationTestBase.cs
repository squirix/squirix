using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Core;
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Xunit;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Base class for squirix integration tests.
/// Provides helpers for starting nodes, building entries,
/// and creating test-scoped persistence directories.
/// </summary>
public abstract class NodeIntegrationTestBase : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> CleanedScopes = new(StringComparer.OrdinalIgnoreCase);
    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();
    private HttpClient? _httpClient;

    private ClusterTls? _mtls;

    static NodeIntegrationTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", NodePathKit.GetProcTempPath());
    }

    /// <summary>
    /// Gets a default <see cref="CancellationToken" /> with a 30s timeout,
    /// recreated lazily on first access.
    /// </summary>
    protected static CancellationToken DefaultCancellationToken => TestContext.Current.CancellationToken;

    /// <summary>
    /// Gets a reusable <see cref="HttpClient" /> for REST and health probes.
    /// </summary>
    protected HttpClient HttpClient => _httpClient ??= CreateHttpClient();

    /// <summary>Cleans up sockets handler, HTTP client, and cancellation tokens.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Builds cluster peer entries, provisioning inter-node mTLS URLs for multi-node topologies.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    internal ServerPeer[] BuildClusterPeers(ReadOnlySpan<(string NodeId, Uri Uri)> topology) => ClusterTls.CreatePeers(ref _mtls, topology);

    /// <summary>Creates an outbound handler that trusts the cluster CA but does not present a client certificate.</summary>
    /// <param name="targetPeerNodeId">Configured node identifier for the peer being contacted.</param>
    /// <param name="peers">Configured cluster peers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handler for negative mTLS inter-node auth tests.</returns>
    internal async Task<SocketsHttpHandler> CreateClusterCaTrustingHandlerNoClientCertAsync(
        string targetPeerNodeId,
        ServerPeer[] peers,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPeerNodeId);
        var bootstrapPeer = peers[0];
        var cluster = new TopologyOptions(peers)
        {
            NodeId = bootstrapPeer.NodeId,
            Uri = bootstrapPeer.Uri,
            VirtualNodes = 128,
        };
        (_mtls, _, var material) = await ClusterTls.ResolveForNodeAsync(_mtls, cluster, bootstrapPeer.Uri, cancellationToken).ConfigureAwait(false);
        return material is not { Enabled: true, TrustAnchor: not null } ? LoopbackHttp.CreateHandler()
            : TestCertificates.CreateClusterCaTrustingHandlerNoClientCert(material.TrustAnchor, targetPeerNodeId);
    }

    /// <summary>Creates an outbound handler that presents a trusted cluster peer certificate for inter-node gRPC.</summary>
    /// <param name="callerNodeId">Configured node identifier for the presenting peer.</param>
    /// <param name="callerPrimaryUrl">Primary listen URL for the presenting peer.</param>
    /// <param name="targetPeerNodeId">Configured node identifier for the peer being contacted.</param>
    /// <param name="peers">Configured cluster peers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handler for trusted inter-node mTLS tests.</returns>
    internal async Task<SocketsHttpHandler> CreateTrustedInterNodeClientHandlerAsync(
        string callerNodeId,
        Uri callerPrimaryUrl,
        string targetPeerNodeId,
        ServerPeer[] peers,
        CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(callerNodeId);
        ArgumentNullException.ThrowIfNull(callerPrimaryUrl);
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPeerNodeId);
        var cluster = new TopologyOptions(peers)
        {
            NodeId = callerNodeId,
            Uri = callerPrimaryUrl,
            VirtualNodes = 128,
        };
        (_mtls, _, var material) = await ClusterTls.ResolveForNodeAsync(_mtls, cluster, callerPrimaryUrl, cancellationToken).ConfigureAwait(false);
        return material is not { Enabled: true } ? LoopbackHttp.CreateHandler()
            : TestCertificates.CreateMtlsHandler(material.NodeCertificate!, material.TrustAnchor!, targetPeerNodeId);
    }

    internal ValueTask<TestNodeHost> StartNodeAsync(string uri, string nodeId, NodeStartOptions? options = null, [CallerMemberName] string? testName = null) =>
        StartNodeAsync(uri, BuildClusterPeer(nodeId, new Uri(uri, UriKind.Absolute)), options, testName);

    internal ValueTask<TestNodeHost> StartNodeAsync(Uri uri, string nodeId, NodeStartOptions? options = null, [CallerMemberName] string? testName = null) =>
        StartNodeAsync(uri, BuildClusterPeer(nodeId, uri), options, testName);

    internal async ValueTask<TestNodeHost> StartNodeAsync(Uri uri, ServerPeer[] peers, NodeStartOptions? options = null, [CallerMemberName] string? testName = null)
    {
        options ??= new NodeStartOptions();
        ArgumentNullException.ThrowIfNull(uri);
        var canonicalUri = new Uri(ListenUris.CanonicalAuthority(uri), UriKind.Absolute);
        var selfNodeId = FindSelfNodeId(peers, canonicalUri) ?? throw new ArgumentException("The peers list must contain an entry for the node being started", nameof(peers));

        var clusterConfig = new TopologyOptions(peers)
        {
            NodeId = selfNodeId,
            Uri = canonicalUri,
            VirtualNodes = 128,
            ReplicaCount = options.ReplicaCount,
            ConfigurationGeneration = options.ConfigurationGeneration,
        };

        var scopeName = TestPersistenceScope.ResolvePersistenceScopeSegment(testName);
        PersistenceOptions? persistenceOptionsOverride = null;
        var dataDir = string.Empty;
        if (options.UsePersistence || options.PersistenceOptions is not null)
        {
            persistenceOptionsOverride = await GetPersistenceOptionsAsync(
                options.PersistenceOptions,
                selfNodeId,
                BuildTestScope(scopeName, options.ExtraScope),
                options.CleanTestDir,
                DefaultCancellationToken);
            dataDir = persistenceOptionsOverride.DataDir;
        }

        (_mtls, var mtlsOptions, var mtlsMaterial) = await ClusterTls.ResolveForNodeAsync(_mtls, clusterConfig, canonicalUri, DefaultCancellationToken);

        var application = await NodeHost.StartAsync(
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
                WaitForRecovery = options.WaitForRecovery,
                ServicesConfigure = options.ServicesConfigure,
                PersistenceOptions = persistenceOptionsOverride,
                SecurityOptions = options.Security?.ToServerOptions(),
                MtlsOptions = mtlsOptions,
                MtlsMaterial = mtlsMaterial,
            },
            DefaultCancellationToken);

        return new TestNodeHost(application, canonicalUri, dataDir, persistenceOptionsOverride is not null);
    }

    /// <summary>Allocates a dedicated port reserved for the lifetime of the test process.</summary>
    /// <returns>A port number reserved from the shared in-process pool.</returns>
    protected static int AllocateDedicatedPort() => ListenPortPool.IntegrationTests.AllocatePort();

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

    /// <summary>Allocates a unique loopback HTTPS listen URI for the next node using the shared port pool.</summary>
    /// <returns>A loopback HTTPS listen URI.</returns>
    protected static Uri GetNextHttpUri() => ListenPortPool.IntegrationTests.NextHttpUri();

    /// <summary>Cleans up managed resources owned by the integration test base.</summary>
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
    /// The value to store. If a <see cref="JsonDocument" /> or <see cref="JsonElement" /> is supplied,
    /// it is cloned to detach from the underlying document’s lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">
    /// Optional absolute UTC expiration time. When <see langword="null" />, the entry does not have an absolute expiry.
    /// </param>
    /// <param name="version">
    /// The initial monotonic version to assign to the entry. Defaults to <c>1</c>.
    /// </param>
    /// <param name="tags">Optional set of user-defined tags. When provided, the collection is frozen using an ordinal string comparer.</param>
    /// <returns>
    /// A new <see cref="NodeCacheEntry{T}" /> instance with the provided <paramref name="value" />, <paramref name="expiresUtc" />,
    /// <paramref name="version" />, and <paramref name="tags" />; <c>Expiration</c> is set to <see langword="null" />.
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

    /// <summary>Resolves the cluster-aware cache API client from the test node’s dependency injection container.</summary>
    /// <param name="host">The started test node host providing access to the service provider.</param>
    /// <returns>The resolved <see cref="ICacheApi{T}" /> instance.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <see cref="ICacheApi{T}" /> is not registered in the node’s service provider.
    /// </exception>
    private protected static ILogicalNamespacedCache<object?> GetCache(TestNodeHost host) => host.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("default");

    private static string BuildTestScope(string? testName, string? extra)
    {
        var baseName = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
        var scope = string.IsNullOrWhiteSpace(extra) ? baseName : $"{baseName}__{extra}";

        var tfm = AppContext.TargetFrameworkName;
        if (!string.IsNullOrWhiteSpace(tfm))
            scope = $"{scope}__{tfm}";

        return $"{scope}__pid{NodeInvariantIndexStrings.Format(Environment.ProcessId)}";
    }

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
    private ServerPeer[] BuildClusterPeer(string nodeId, Uri uri) => ClusterTls.CreatePeer(ref _mtls, nodeId, uri);

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };

    private async Task<PersistenceOptions> GetPersistenceOptionsAsync(
        PersistenceOptions? persistenceOptions,
        string selfNodeId,
        string testScope,
        bool clean,
        CancellationToken cancellationToken)
    {
        var path = NodePathKit.Combine(true, NodePathKit.GetProcTempPath(), GetType().Name, testScope, "cluster");
        if (clean && CleanedScopes.TryAdd(path, 0))
            await DirectoryKit.DeleteDirectoryAsync(path, cancellationToken).ConfigureAwait(false);

        var effectiveDataDir = string.IsNullOrWhiteSpace(persistenceOptions?.DataDir) ? NodePathKit.Combine(true, path, selfNodeId) : persistenceOptions.DataDir;
        DirectoryKit.CreateDirectory(effectiveDataDir);

        if (persistenceOptions is null)
            return new PersistenceOptions
            {
                DataDir = effectiveDataDir,
                JournalMaxSegmentMb = 64,
            };

        return string.IsNullOrWhiteSpace(persistenceOptions.DataDir) ? persistenceOptions with { DataDir = effectiveDataDir } : persistenceOptions;
    }

    /// <summary>
    /// Starts a new <see cref="NodeHost" /> for integration testing with configurable peers,
    /// persistence, gRPC configuration, and extra services.
    /// </summary>
    /// <param name="uri">
    /// The node’s listen URL (HTTP or HTTPS). Must correspond to one of the <paramref name="peers" /> entries.
    /// </param>
    /// <param name="peers">
    /// The cluster peer set, including the node being started (its <see cref="ServerPeer.Uri" /> must equal <paramref name="uri" />).
    /// </param>
    /// <param name="options">Optional startup knobs (persistence, security, policies, etc.).</param>
    /// <param name="testName">
    /// Optional scope hint from the caller (often via <see cref="CallerMemberNameAttribute" />).
    /// Under xUnit, <see cref="TestPersistenceScope.ResolvePersistenceScopeSegment" /> uses the active test case id when available.
    /// </param>
    /// <returns>
    /// A started <see cref="TestNodeHost" /> wrapper containing the running application, its base URL, and the resolved data directory.
    /// Dispose it to stop the node and release resources.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="peers" /> does not contain an entry for <paramref name="uri" /> (the self node).
    /// </exception>
    private ValueTask<TestNodeHost> StartNodeAsync(string uri, ServerPeer[] peers, NodeStartOptions? options = null, [CallerMemberName] string? testName = null) =>
        StartNodeAsync(new Uri(uri, UriKind.Absolute), peers, options, testName);
}
