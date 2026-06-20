using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Grpc.Net.Client;
using JetBrains.Annotations;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Contracts;
using Squirix.Server.Limits;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.Environment;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.TestKit.XUnit;
using Xunit;

namespace Squirix.Server.SmokeTests.Support;

/// <summary>
/// Base class for all smoke tests, providing helper methods to start test nodes,
/// manage test directories, construct HTTP clients, and build common cache entries.
/// </summary>
[SuppressMessage("Maintainability", "CA1515:Consider making public types internal", Justification = "Unit test base class must be public")]
public abstract class SmokeTestBase : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> CleanedScopes = new(StringComparer.Ordinal);

    private static readonly TestNodeSecurityOptions UnauthenticatedSecurity = new();

    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();
    private HttpClient? _httpClient;

    private MtlsTestContext? _mtls;

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

    /// <summary>Resolves the cluster-aware cache API client from the node's dependency injection container.</summary>
    /// <param name="host">The started test node host that exposes the service provider.</param>
    /// <returns>
    /// The resolved <see cref="ICacheApi{T}" /> instance to interact with the node.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <see cref="ICacheApi{T}" /> is not registered in the node's service provider.
    /// </exception>
    internal static ICacheApi<object?> GetCacheApiClient(TestNodeHost host) => host.Services.GetRequiredService<ICacheApi<object?>>();

    /// <summary>Gets the next available HTTP URL bound to 127.0.0.1 with a dynamically allocated port.</summary>
    /// <returns>
    /// A loopback URL of the form <c>https://127.0.0.1:&lt;port&gt;</c>, where <c>&lt;port&gt;</c>
    /// is reserved from the shared port pool at the time of the call.
    /// </returns>
    /// <remarks>
    /// The port is allocated by the test process and is intended for ephemeral use during integration tests.
    /// Callers should bind immediately to minimize races with other processes.
    /// </remarks>
    /// <summary>Builds cluster peer entries, provisioning inter-node mTLS URLs for multi-node topologies.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>Peer entries for host startup.</returns>
    internal Peer[] BuildClusterPeers(params (string NodeId, Uri Url)[] topology)
    {
        var mapped = new (string NodeId, string Url)[topology.Length];
        for (var i = 0; i < topology.Length; i++)
            mapped[i] = (topology[i].NodeId, ListenUrls.CanonicalAuthority(topology[i].Url));

        return MtlsTestContext.CreatePeers(ref _mtls, mapped);
    }

    /// <summary>
    /// Starts a new <see cref="SquirixNodeHost" /> instance configured for testing,
    /// using the provided peers, persistence, snapshot and service options.
    /// </summary>
    /// <param name="url">The URL this node should bind to.</param>
    /// <param name="peers">Cluster peers including this node.</param>
    /// <param name="callPolicyFactory">Optional factory for client call policies.</param>
    /// <param name="configureGrpc">Optional action to configure gRPC options.</param>
    /// <param name="servicesConfigure">Optional action to configure DI services.</param>
    /// <param name="snapshotOptions">Optional snapshot trigger options.</param>
    /// <param name="persistenceOptions">Optional persistence options.</param>
    /// <param name="usePersistence">When <see langword="true" />, starts the node with journal/snapshot persistence enabled.</param>
    /// <param name="output">Optional xUnit output helper for log capture.</param>
    /// <param name="cleanTestDir">Whether to clean the test directory before starting.</param>
    /// <param name="extraScope">Optional extra scope string for test directory isolation.</param>
    /// <param name="security">
    /// Per-node security override. Defaults to unauthenticated when omitted. Environment variables are not read for auth when an override is supplied.
    /// </param>
    /// <param name="backpressureOptions">Optional backpressure options for inbound admission control.</param>
    /// <param name="memoryPressureOptions">Optional memory pressure options; when <see langword="null" />, defaults merged from settings and environment are used.</param>
    /// <param name="testName">
    /// Optional caller hint; under xUnit, <see cref="TestPersistenceScope.ResolvePersistenceScopeSegment" /> prefers the active test case id.
    /// </param>
    /// <param name="cancellationToken">Cancellation token to stop startup.</param>
    /// <returns>A started <see cref="TestNodeHost" /> wrapper around the node.</returns>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The node host client pool owns the handler for the process lifetime of the test node.")]
    internal ValueTask<TestNodeHost> StartNodeAsync(
        string url,
        Peer[] peers,
        Func<string, CallPolicy>? callPolicyFactory = null,
        Action<GrpcServiceOptions>? configureGrpc = null,
        Action<IServiceCollection>? servicesConfigure = null,
        SnapshotTriggerOptions? snapshotOptions = null,
        PersistenceOptions? persistenceOptions = null,
        bool usePersistence = false,
        ITestOutputHelper? output = null,
        bool cleanTestDir = true,
        string? extraScope = null,
        TestNodeSecurityOptions? security = null,
        BackpressureOptions? backpressureOptions = null,
        MemoryPressureOptions? memoryPressureOptions = null,
        [CallerMemberName] string? testName = null,
        CancellationToken cancellationToken = default) => StartNodeAsync(
        new Uri(url, UriKind.Absolute),
        peers,
        callPolicyFactory,
        configureGrpc,
        servicesConfigure,
        snapshotOptions,
        persistenceOptions,
        usePersistence,
        output,
        cleanTestDir,
        extraScope,
        security,
        backpressureOptions,
        memoryPressureOptions,
        testName,
        cancellationToken);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The node host client pool owns the handler for the process lifetime of the test node.")]
    internal async ValueTask<TestNodeHost> StartNodeAsync(
        Uri url,
        Peer[] peers,
        Func<string, CallPolicy>? callPolicyFactory = null,
        Action<GrpcServiceOptions>? configureGrpc = null,
        Action<IServiceCollection>? servicesConfigure = null,
        SnapshotTriggerOptions? snapshotOptions = null,
        PersistenceOptions? persistenceOptions = null,
        bool usePersistence = false,
        ITestOutputHelper? output = null,
        bool cleanTestDir = true,
        string? extraScope = null,
        TestNodeSecurityOptions? security = null,
        BackpressureOptions? backpressureOptions = null,
        MemoryPressureOptions? memoryPressureOptions = null,
        [CallerMemberName] string? testName = null,
        CancellationToken cancellationToken = default)
    {
        var urlString = ListenUrls.CanonicalAuthority(url);
        var selfNodeId = peers.FirstOrDefault(p => ListenUrls.SameAuthority(p.Url, urlString))?.NodeId ?? throw new ArgumentException(
            "The peers list must contain an entry for the node being started",
            nameof(peers));

        var clusterConfig = new ClusterConfig
        {
            NodeId = selfNodeId,
            Url = urlString,
            VirtualNodes = 128,
            Peers = peers,
        };

        var scope = BuildTestScope(TestPersistenceScope.ResolvePersistenceScopeSegment(testName), extraScope);
        PersistenceOptions? persistenceOptionsOverride = null;
        var dataDir = string.Empty;
        if (usePersistence || persistenceOptions is not null)
        {
            persistenceOptionsOverride = await GetPersistenceOptionsAsync(persistenceOptions, selfNodeId, scope, cleanTestDir, cancellationToken).ConfigureAwait(false);
            dataDir = persistenceOptionsOverride.DataDir;
        }

        (_mtls, var mtlsOptions, var mtlsMaterial) = await MtlsTestContext.ResolveForNodeAsync(_mtls, clusterConfig, urlString, cancellationToken).ConfigureAwait(false);
        var app = await SquirixNodeHost.StartAsync(
            clusterConfig,
            b =>
            {
                _ = b.ClearProviders();
                _ = b.SetMinimumLevel(LogLevel.Debug);
                _ = b.AddFilter("Grpc", LogLevel.Debug);
                _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Debug);
                _ = b.AddFilter("Squirix", LogLevel.Debug);
                _ = output is not null ? b.AddProvider(new XUnitLoggerProvider(output)) : b.AddConsole().AddDebug();
            },
            true,
            snapshotOptions,
            callPolicyFactory,
            configureGrpc,
            servicesConfigure,
            persistenceOptionsOverride,
            null,
            backpressureOptions,
            memoryPressureOptions,
            (security ?? UnauthenticatedSecurity).ToServerOptions(),
            null,
            mtlsOptions,
            mtlsMaterial,
            cancellationToken);

        return new TestNodeHost(app, urlString, dataDir, persistenceOptionsOverride is not null);
    }

    /// <summary>Creates a gRPC channel configured for HTTPS against a test node URL.</summary>
    /// <param name="url">The node listen URL.</param>
    /// <returns>A disposable gRPC channel.</returns>
    protected static GrpcChannel CreateGrpcChannel(Uri url) => GrpcChannel.ForAddress(
        url,
        new GrpcChannelOptions
        {
            HttpHandler = LoopbackHttp.CreateHandler(),
            MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
            MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
        });

    /// <summary>
    /// Gets listen URLs for a node bound on all interfaces (<c>0.0.0.0</c>) and scraped via loopback.
    /// </summary>
    /// <returns>A tuple of bind URL and loopback scrape URL sharing the same port.</returns>
    protected static (string BindUrl, string LoopbackUrl) GetNextAnyInterfaceListenUrls()
    {
        var port = ListenPortPool.SmokeTests.AllocatePort();
        return ($"https://0.0.0.0:{port.ToString(CultureInfo.InvariantCulture)}", $"https://127.0.0.1:{port.ToString(CultureInfo.InvariantCulture)}");
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
    /// Convenience builder for a <see cref="CacheEntry{T}" /> with optional expiration, version, and tags.
    /// </summary>
    /// <param name="value">
    /// The value to store. If a JsonDocument or JsonElement is supplied, it is cloned to detach from the
    /// underlying document's lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">
    /// Optional absolute UTC expiration time. When <see langword="null" />, the entry has no absolute expiry.
    /// </param>
    /// <param name="version">
    /// The initial monotonic version to assign to the entry. Defaults to <c>1</c>.
    /// </param>
    /// <param name="tags">
    /// Optional set of user-defined tags. When provided, the collection is defensively copied
    /// using an ordinal string comparer to prevent external mutation.
    /// </param>
    /// <returns>
    /// A new <see cref="CacheEntry{T}" /> containing the provided <paramref name="value" />,
    /// <paramref name="expiresUtc" />, <paramref name="version" />, and <paramref name="tags" /> (if any).
    /// The <c>Expiration</c> property is set to <see langword="null" />.
    /// </returns>
    private protected static CacheEntry<object?> BuildEntry(object? value, DateTime? expiresUtc = null, long version = 1, IDictionary<string, string>? tags = null)
    {
        var v = value switch
        {
            JsonDocument doc => doc.RootElement.Clone(),
            JsonElement elem => elem.Clone(),
            _ => value,
        };

        return new CacheEntry<object?>
        {
            Value = v,
            ExpiresUtc = expiresUtc,
            Expiration = null,
            Version = version,
            Tags = tags?.ToFrozenDictionary(StringComparer.Ordinal),
        };
    }

    private static string BuildTestScope(string? testName, string? extra)
    {
        var baseName = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
        var combined = string.IsNullOrWhiteSpace(extra) ? baseName : $"{baseName}__{extra}";
        return $"{combined}__pid{Environment.ProcessId.ToString(CultureInfo.InvariantCulture)}";
    }

    /// <summary>
    /// Gets the root directory for test persistence. Uses <c>XUNIT_TEST_ROOT</c> env variable if set,
    /// otherwise falls back to <c>%LOCALAPPDATA%\SquirixSmoke</c>.
    /// </summary>
    /// <returns>A stable root path for smoke-test data.</returns>
    private static string GetStableRoot()
    {
        var fromEnv = EnvVar.Get("XUNIT_TEST_ROOT");
        if (!string.IsNullOrWhiteSpace(fromEnv))
            return fromEnv;

        var appData = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);
        return PathKit.Combine(true, appData, "SquirixSmoke");
    }

    private async Task<string> ConstructDataDirAsync(string? dataDir, string selfNodeId, string testScope, bool clean, CancellationToken cancellationToken)
    {
        var dataRoot = PathKit.Combine(true, GetStableRoot(), GetType().Name, testScope, "cluster");
        if (clean && CleanedScopes.TryAdd(dataRoot, 0))
            await DirectoryKit.TryDeleteDirectoryAsync(dataRoot, cancellationToken).ConfigureAwait(false);

        var combine = dataDir ?? PathKit.Combine(true, dataRoot, selfNodeId);
        DirectoryKit.CreateDirectory(combine);
        return combine;
    }

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
        var dataDir = await ConstructDataDirAsync(persistenceOptions?.DataDir, selfNodeId, testScope, clean, cancellationToken).ConfigureAwait(false);
        return persistenceOptions ?? new PersistenceOptions
        {
            DataDir = dataDir,
            JournalMaxSegmentMb = 64,
            FlushIntervalMs = 10,
            SnapshotIntervalSec = 60,
        };
    }
}
