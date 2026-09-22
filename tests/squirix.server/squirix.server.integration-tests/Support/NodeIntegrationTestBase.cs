using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.IO;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using JetBrains.Annotations;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;

namespace Squirix.Server.IntegrationTests.Support;

/// <summary>
/// Base class for squirix integration tests.
/// Provides helpers for starting nodes, building entries,
/// and creating test-scoped persistence directories.
/// </summary>
public abstract class NodeIntegrationTestBase : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> CleanedScopes = new(StringComparer.OrdinalIgnoreCase);
    private static readonly ConcurrentDictionary<string, SemaphoreSlim> ScopeLocks = new(StringComparer.OrdinalIgnoreCase);
    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();
    private HttpClient? _httpClient;

    private ClusterIdentity? _identity;

    static NodeIntegrationTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", NodePathKit.GetProcTempPath());
    }

    /// <summary>Gets a reusable <see cref="HttpClient" /> for REST and health probes.</summary>
    protected HttpClient HttpClient => _httpClient ??= CreateHttpClient();

    /// <summary>Cleans up socket handler and HTTP client.</summary>
    public void Dispose()
    {
        Dispose(true);
        GC.SuppressFinalize(this);
    }

    /// <summary>Builds cluster peer entries, provisioning internode mTLS URLs for multi-node topologies.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    internal ServerPeer[] BuildClusterPeers(ClusterNode[] topology) => ClusterIdentity.CreatePeers(topology, ref _identity);

    /// <summary>Creates an outbound handler that trusts the cluster CA but does not present a client certificate.</summary>
    /// <param name="targetPeerNodeId">Configured node identifier for the peer being contacted.</param>
    /// <param name="peers">Configured cluster peers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handler for negative mTLS internode auth tests.</returns>
    internal async Task<SocketsHttpHandler> CreateCaTrustingHandlerAsync(string targetPeerNodeId, IReadOnlyList<ServerPeer> peers, CancellationToken cancellationToken = default)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(targetPeerNodeId);
        var bootstrapPeer = peers[0];
        var cluster = new TopologyOptions(peers)
        {
            NodeId = bootstrapPeer.NodeId,
            Uri = bootstrapPeer.Uri,
            VirtualNodes = 128,
        };
        (_identity, _, var material) = await ClusterIdentity.ResolveForNodeAsync(_identity, cluster, cancellationToken).ConfigureAwait(false);
        return material is not { Enabled: true, TrustAnchor: not null } ? LoopbackHttp.CreateHandler()
            : TestCertificates.CreateCaTrustingHandlerNoClientCert(material.TrustAnchor, targetPeerNodeId);
    }

    /// <summary>Creates an outbound handler that presents a trusted cluster peer certificate for internode gRPC.</summary>
    /// <param name="callerNodeId">Configured node identifier for the presenting peer.</param>
    /// <param name="callerPrimaryUrl">Primary listen URL for the presenting peer.</param>
    /// <param name="targetPeerNodeId">Configured node identifier for the peer being contacted.</param>
    /// <param name="peers">Configured cluster peers.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A handler for trusted internode mTLS tests.</returns>
    internal async Task<SocketsHttpHandler> CreateTrustedInterNodeClientHandlerAsync(
        string callerNodeId,
        Uri callerPrimaryUrl,
        string targetPeerNodeId,
        IReadOnlyList<ServerPeer> peers,
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
        (_identity, _, var material) = await ClusterIdentity.ResolveForNodeAsync(_identity, cluster, cancellationToken).ConfigureAwait(false);
        return material is not { Enabled: true } ? LoopbackHttp.CreateHandler()
            : TestCertificates.CreateMtlsHandler(material.NodeCertificate!, material.TrustAnchor!, targetPeerNodeId);
    }

    /// <summary>Reserves one loopback listen URI and starts a single-node cluster.</summary>
    /// <param name="nodeId">Node identifier to start.</param>
    /// <param name="options">Optional startup knobs applied to the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="testName">Optional persistence scope hint from the caller.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal ValueTask<TestCluster<IntegrationStartOptions>> StartClusterAsync(
        string nodeId,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null) => StartClusterAsync([new ClusterNode(nodeId, GetNextHttpUri())], options, cancellationToken, testName);

    /// <summary>Reserves one loopback listen URI per node and starts a two-node cluster.</summary>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="options">Optional startup knobs applied to every node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="testName">Optional persistence scope hint from the caller.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal ValueTask<TestCluster<IntegrationStartOptions>> StartClusterAsync(
        string nodeA,
        string nodeB,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null) => StartClusterAsync(
        [new ClusterNode(nodeA, GetNextHttpUri()), new ClusterNode(nodeB, GetNextHttpUri())],
        options,
        cancellationToken,
        testName);

    /// <summary>Reserves one loopback listen URI per node and starts a three-node cluster.</summary>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="nodeC">Third node identifier.</param>
    /// <param name="options">Optional startup knobs applied to every node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="testName">Optional persistence scope hint from the caller.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal ValueTask<TestCluster<IntegrationStartOptions>> StartClusterAsync(
        string nodeA,
        string nodeB,
        string nodeC,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null) => StartClusterAsync(
        [new ClusterNode(nodeA, GetNextHttpUri()), new ClusterNode(nodeB, GetNextHttpUri()), new ClusterNode(nodeC, GetNextHttpUri())],
        options,
        cancellationToken,
        testName);

    /// <summary>Starts a single-node cluster for the supplied topology entry.</summary>
    /// <param name="node">Node identifier paired with its listen URI.</param>
    /// <param name="options">Optional startup knobs applied to the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="testName">Optional persistence scope hint from the caller.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal ValueTask<TestCluster<IntegrationStartOptions>> StartClusterAsync(
        ClusterNode node,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null) => StartClusterAsync([node], options, cancellationToken, testName);

    /// <summary>Starts one node per topology entry with a shared peer set.</summary>
    /// <param name="topology">Node identifiers paired with their listen URIs, in start order.</param>
    /// <param name="options">Optional startup knobs applied to every node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <param name="testName">Optional persistence scope hint from the caller.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal async ValueTask<TestCluster<IntegrationStartOptions>> StartClusterAsync(
        ClusterNode[] topology,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        ArgumentNullException.ThrowIfNull(topology);
        var peers = BuildClusterPeers(topology);
        TestCluster<IntegrationStartOptions>? cluster = null;
        try
        {
            cluster = TestCluster<IntegrationStartOptions>.Create(
                topology,
                (self, nodeTopology, nodeOptions, token) => StartClusterAsync(self.Uri, BuildClusterPeers(nodeTopology), nodeOptions, token, testName),
                peers);

            var started = await cluster.StartAllAsync(_ => options, i => ListenPortPool.IntegrationTests.ReleasePort(topology[i].Uri.Port), cancellationToken)
                                       .ConfigureAwait(false);
            cluster = null;
            return started;
        }
        finally
        {
            if (cluster != null)
                await cluster.DisposeAsync().ConfigureAwait(false);
        }
    }

    internal async ValueTask<ITestNodeHost> StartClusterAsync(
        Uri uri,
        IReadOnlyList<ServerPeer> peers,
        IntegrationStartOptions? options = null,
        CancellationToken cancellationToken = default,
        [CallerMemberName] string? testName = null)
    {
        options ??= new IntegrationStartOptions();
        Helpers.ThrowIfUnsupportedClusterStartOptions(options);
        ArgumentNullException.ThrowIfNull(uri);
        var canonicalUri = new Uri(ListenUris.CanonicalAuthority(uri), UriKind.Absolute);
        var selfNodeId = Helpers.FindSelfNodeId(peers, canonicalUri) ??
                         ThrowHelper.Throw<string>(new ArgumentException("The peers list must contain an entry for the node being started", nameof(peers)));

        var config = new TopologyOptions(peers)
        {
            NodeId = selfNodeId,
            Uri = canonicalUri,
            VirtualNodes = 128,
            ReplicaCount = options.ReplicaCount,
            ReplicationEnabled = options.EnableReplication,
            ConfigurationGeneration = options.ConfigurationGeneration,
        };

        try
        {
            var name = TestPersistenceScope.ResolvePersistenceScopeSegment(testName);
            PersistenceOptions? po = null;
            var dir = string.Empty;
            if (options.UsePersistence || options.PersistenceOptions != null)
            {
                po = await GetPersistenceOptionsAsync(
                    options.PersistenceOptions,
                    selfNodeId,
                    Helpers.BuildTestScope(name, options.ExtraScope),
                    options.CleanTestDir,
                    cancellationToken);
                dir = po.DataDir;
            }

            (_identity, var mtlsOptions, var mtlsMaterial) = await ClusterIdentity.ResolveForBindAsync(_identity, config, cancellationToken);

            var startOptions = Helpers.CreateStartOptions(options, po, mtlsOptions, mtlsMaterial);
            ListenPortPool.IntegrationTests.ReleasePort(canonicalUri.Port);
            var application = await NodeHost.StartAsync(config, startOptions, cancellationToken);
            return new TestNodeHost(application, canonicalUri, dir, po != null);
        }
        catch
        {
            ListenPortPool.IntegrationTests.ReleasePort(canonicalUri.Port);
            throw;
        }
    }

    /// <summary>Allocates a dedicated port, held bound until <see cref="StartClusterAsync(System.Uri,System.Collections.Generic.IReadOnlyList{Squirix.Server.Cluster.ServerPeer},Squirix.Server.IntegrationTests.Support.IntegrationStartOptions?,System.Threading.CancellationToken,string?)" /> releases it for the real bind.</summary>
    /// <returns>A held loopback port; the hold is released by node startup, disposing it earlier releases it manually.</returns>
    protected static HeldPort AllocateDedicatedPort() => ListenPortPool.IntegrationTests.HoldPort();

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

    /// <summary>Allocates a unique loopback HTTPS listen URI, held bound until <see cref="StartClusterAsync(System.Uri,System.Collections.Generic.IReadOnlyList{Squirix.Server.Cluster.ServerPeer},Squirix.Server.IntegrationTests.Support.IntegrationStartOptions?,System.Threading.CancellationToken,string?)" /> releases it for the real bind.</summary>
    /// <returns>A loopback HTTPS listen URI.</returns>
    protected static Uri GetNextHttpUri() => ListenPortPool.IntegrationTests.HoldHttpUri();

    /// <summary>Cleans up managed resources owned by the integration test base.</summary>
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
    /// The value to store. If a <see cref="JsonDocument" /> or <see cref="JsonElement" /> is supplied,
    /// it is cloned to detach from the underlying document’s lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">Optional absolute UTC expiration time. When <see langword="null" />, the entry does not have absolute expiry.</param>
    /// <param name="version">The initial monotonic version to assign to the entry. Defaults to <c language="csharp">1</c>.</param>
    /// <param name="tags">Optional set of user-defined tags. When provided, the collection is frozen using an ordinal string comparer.</param>
    /// <returns>
    /// A new <see cref="NodeCacheEntry{T}" /> instance with the provided <paramref name="value" />, <paramref name="expiresUtc" />,
    /// <paramref name="version" />, and <paramref name="tags" />; <c language="csharp">Expiration</c> is set to <see langword="null" />.
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
    /// <exception cref="InvalidOperationException">Thrown if <see cref="ICacheApi{T}" /> is not registered in the node’s service provider.</exception>
    private protected static ILogicalNamespacedCache<object?> GetCache(ITestNodeHost host) => host.GetCache<object?>("default");

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };

    private async Task<PersistenceOptions> GetPersistenceOptionsAsync(PersistenceOptions? options, string nodeId, string testScope, bool clean, CancellationToken cancellationToken)
    {
        PathValidationKit.ValidateSegmentName(testScope, nameof(testScope));
        PathValidationKit.ValidateSegmentName(nodeId, nameof(nodeId));
        if (!string.IsNullOrWhiteSpace(options?.DataDir))
            PathValidationKit.ValidateNoParentSegments(options.DataDir, nameof(options));

        var path = NodePathKit.Combine(true, NodePathKit.GetProcTempPath(), GetType().Name, testScope, "cluster");

        // Serialize cleanup-and-creation per scope so a concurrent node start can never create
        // its node directory while another caller is deleting the scope.
        var scopeLock = ScopeLocks.GetOrAdd(path, static _ => new SemaphoreSlim(1, 1));
        await scopeLock.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            if (clean && CleanedScopes.TryAdd(path, 0))
            {
                try
                {
                    if (Directory.Exists(path))
                        Directory.Delete(path, true);
                }
                catch (Exception)
                {
                    _ = CleanedScopes.TryRemove(path, out _);
                    throw;
                }
            }

            var dir = string.IsNullOrWhiteSpace(options?.DataDir) ? NodePathKit.Combine(true, path, nodeId) : options.DataDir;
            Directory.CreateDirectory(dir);

            return options switch
            {
                null => new PersistenceOptions
                {
                    DataDir = dir,
                    JournalMaxSegmentMb = 64,
                },
                _ when string.IsNullOrWhiteSpace(options.DataDir) => options with { DataDir = dir },
                _ => options,
            };
        }
        finally
        {
            _ = scopeLock.Release();
        }
    }

    private static class Helpers
    {
        internal static string BuildTestScope(string? testName, string? extra)
        {
            var name = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
            var scope = string.IsNullOrWhiteSpace(extra) ? name : $"{name}__{extra}";

            var tfm = AppContext.TargetFrameworkName;
            if (!string.IsNullOrWhiteSpace(tfm))
                scope = $"{scope}__{tfm}";

            return $"{scope}__pid{NodeInvariantIndexStrings.Format(Environment.ProcessId)}";
        }

        internal static NodeHostStartOptions CreateStartOptions(
            IntegrationStartOptions options,
            PersistenceOptions? persistenceOptions,
            MtlsOptions? mtlsOptions,
            MtlsCertificate? certificate)
        {
            return new NodeHostStartOptions
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
                PersistenceOptions = persistenceOptions,
                SecurityOptions = options.Security?.ToServerOptions(),
                MtlsOptions = mtlsOptions,
                Certificate = certificate,
                FoundationOnly = options.FoundationOnly,
            };
        }

        internal static string? FindSelfNodeId(IReadOnlyList<ServerPeer> peers, Uri uri)
        {
            ArgumentNullException.ThrowIfNull(uri);
            for (var index = 0; index < peers.Count; index++)
            {
                var peer = peers[index];
                if (ListenUris.SameAuthority(peer.Uri, uri))
                    return peer.NodeId;
            }

            return null;
        }

        /// <summary>Fails loudly on <see cref="ClusterStartOptions" /> members this starter does not wire into node startup.</summary>
        /// <param name="options">The options to validate.</param>
        /// <exception cref="NotSupportedException">Thrown when an unsupported member is set to a non-default value.</exception>
        internal static void ThrowIfUnsupportedClusterStartOptions(IntegrationStartOptions options)
        {
            if (options.DataDir != null || options.MtlsProfile != TestNodeProfile.Normal || options.TimeProvider != null)
            {
                throw new NotSupportedException(
                    "IntegrationStartOptions does not wire DataDir, MtlsProfile, or TimeProvider into node startup; use PersistenceOptions/UsePersistence for persistence.");
            }
        }
    }
}
