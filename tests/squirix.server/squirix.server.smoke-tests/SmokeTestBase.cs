using System;
using System.Collections.Concurrent;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Runtime.CompilerServices;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Grpc.Net.Client;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Reliability;
using Squirix.Server.Contracts;
using Squirix.Server.Core;
using Squirix.Server.Limits;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.MemoryPressure;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.XUnit;
using Xunit;

namespace Squirix.Server.SmokeTests;

/// <summary>
/// Base class for all smoke tests, providing helper methods to start test nodes,
/// manage test directories, construct HTTP clients, and build common cache entries.
/// </summary>
public abstract class SmokeTestBase : IDisposable
{
    private const int PortRangeSize = 200;
    private static readonly int PortRangeStart = CalculatePortRangeStart();
    private static readonly PortAllocator PortPool = new(PortRangeStart, PortRangeStart + PortRangeSize - 1);
    private static readonly ConcurrentDictionary<string, byte> CleanedScopes = new();

    private static readonly TestNodeSecurityOptions UnauthenticatedSecurity = new();

    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();

    private MtlsTestContext? _mtls;
    private HttpClient? _httpClient;

    /// <summary>
    /// Gets a default cancellation token with a fixed timeout (~30s) for smoke tests.
    /// </summary>
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
        _mtls?.Dispose();
        _socketsHttpHandler.Dispose();
        _httpClient?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Resolves the cluster-aware cache API client from the node's dependency injection container.
    /// </summary>
    /// <param name="host">The started test node host that exposes the service provider.</param>
    /// <returns>
    /// The resolved <see cref="ICacheApi{T}" /> instance to interact with the node.
    /// </returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <see cref="ICacheApi{T}" /> is not registered in the node's service provider.
    /// </exception>
    internal static ICacheApi<object?> GetCacheApiClient(TestNodeHost host) => host.Services.GetRequiredService<ICacheApi<object?>>();

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
    /// <param name="usePersistence">When <c>true</c>, starts the node with WAL/snapshot persistence enabled.</param>
    /// <param name="output">Optional xUnit output helper for log capture.</param>
    /// <param name="cleanTestDir">Whether to clean the test directory before starting.</param>
    /// <param name="extraScope">Optional extra scope string for test directory isolation.</param>
    /// <param name="security">
    /// Per-node security override. Defaults to unauthenticated when omitted. Environment variables are not read for auth when an override is supplied.
    /// </param>
    /// <param name="backpressureOptions">Optional backpressure options for inbound admission control.</param>
    /// <param name="runtimeOptions">Optional cache runtime options such as strict type binding policy.</param>
    /// <param name="memoryPressureOptions">Optional memory pressure options; when <c>null</c>, defaults merged from settings and environment are used.</param>
    /// <param name="testName">
    /// Optional caller hint; under xUnit, <see cref="TestPersistenceScope.ResolvePersistenceScopeSegment" /> prefers the active test case id.
    /// </param>
    /// <param name="cancellationToken">Cancellation token to stop startup.</param>
    /// <returns>A started <see cref="TestNodeHost" /> wrapper around the node.</returns>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The node host client pool owns the handler for the process lifetime of the test node.")]
    internal async ValueTask<TestNodeHost> StartNodeAsync(
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
        CacheRuntimeOptions? runtimeOptions = null,
        MemoryPressureOptions? memoryPressureOptions = null,
        [CallerMemberName] string? testName = null,
        CancellationToken cancellationToken = default)
    {
        var selfNodeId = peers.FirstOrDefault(p => string.Equals(p.Url, url, StringComparison.OrdinalIgnoreCase))?.NodeId ??
                         throw new ArgumentException("The peers list must contain an entry for the node being started", nameof(peers));

        var clusterConfig = new ClusterConfig
        {
            NodeId = selfNodeId,
            Url = url,
            VirtualNodes = 128,
            Peers = peers,
        };

        var scope = BuildTestScope(TestPersistenceScope.ResolvePersistenceScopeSegment(testName), extraScope);
        PersistenceOptions? persistenceOptionsOverride = null;
        var dataDir = string.Empty;
        if (usePersistence || persistenceOptions is not null)
        {
            persistenceOptionsOverride = GetPersistenceOptions(persistenceOptions, selfNodeId, scope, cleanTestDir);
            dataDir = persistenceOptionsOverride.DataDir;
        }

        var (mtlsOptions, mtlsMaterial) = MtlsTestContext.ResolveForNode(ref _mtls, clusterConfig, url);

        var app = await SquirixNodeHost.StartAsync(
            clusterConfig,
            b =>
            {
                _ = b.ClearProviders();
                _ = b.SetMinimumLevel(LogLevel.Debug);
                _ = b.AddFilter("Grpc", LogLevel.Debug);
                _ = b.AddFilter("Grpc.AspNetCore.Server", LogLevel.Debug);
                _ = b.AddFilter("Squirix", LogLevel.Debug);
                _ = output != null ? b.AddProvider(new XUnitKit.XUnitLoggerProvider(output)) : b.AddConsole().AddDebug();
            },
            true,
            snapshotOptions,
            callPolicyFactory,
            configureGrpc,
            servicesConfigure,
            persistenceOptionsOverride,
            LoopbackHttp.CreateHandler(),
            backpressureOptions,
            runtimeOptions,
            memoryPressureOptions,
            (security ?? UnauthenticatedSecurity).ToServerOptions(),
            null,
            mtlsOptions,
            mtlsMaterial,
            cancellationToken);

        return new TestNodeHost(app, url, dataDir, persistenceOptionsOverride is not null);
    }

    /// <summary>
    /// Gets the next available HTTP URL bound to 127.0.0.1 with a dynamically allocated port.
    /// </summary>
    /// <returns>
    /// A loopback URL of the form <c>https://127.0.0.1:&lt;port&gt;</c>, where <c>&lt;port&gt;</c>
    /// is reserved from the shared port pool at the time of the call.
    /// </returns>
    /// <remarks>
    /// The port is allocated by the test process and is intended for ephemeral use during integration tests.
    /// Callers should bind immediately to minimize races with other processes.
    /// </remarks>
    protected static string GetNextHttpUrl() => $"https://127.0.0.1:{PortPool.Allocate()}";

    /// <summary>
    /// Gets listen URLs for a node bound on all interfaces (<c>0.0.0.0</c>) and scraped via loopback.
    /// </summary>
    /// <returns>A tuple of bind URL and loopback scrape URL sharing the same port.</returns>
    protected static (string BindUrl, string LoopbackUrl) GetNextAnyInterfaceListenUrls()
    {
        var port = PortPool.Allocate();
        return ($"https://0.0.0.0:{port}", $"https://127.0.0.1:{port}");
    }

    /// <summary>
    /// Creates a gRPC channel configured for HTTPS against a test node URL.
    /// </summary>
    /// <param name="url">The node listen URL.</param>
    /// <returns>A disposable gRPC channel.</returns>
    protected static GrpcChannel CreateGrpcChannel(string url) => GrpcChannel.ForAddress(
        url,
        new GrpcChannelOptions
        {
            HttpHandler = LoopbackHttp.CreateHandler(),
            MaxReceiveMessageSize = SquirixEntryLimits.GrpcMaxReceiveMessageSizeBytes,
            MaxSendMessageSize = SquirixEntryLimits.GrpcMaxSendMessageSizeBytes,
        });

    /// <summary>
    /// Convenience builder for a <see cref="CacheEntry{T}" /> with optional expiration, version, and tags.
    /// </summary>
    /// <param name="value">
    /// The value to store. If a JsonDocument or JsonElement is supplied, it is cloned to detach from the
    /// underlying document's lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">
    /// Optional absolute UTC expiration time. When <c>null</c>, the entry has no absolute expiry.
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
    /// The <c>Expiration</c> property is set to <c>null</c>.
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
        return $"{combined}__pid{Environment.ProcessId}";
    }

    private static int CalculatePortRangeStart()
    {
        const int port = 40000;
        var maxBuckets = Math.Max(1, (65535 - port) / PortRangeSize);
        var salt = Math.Abs(Environment.ProcessId % maxBuckets);
        return port + (salt * PortRangeSize);
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

    private string ConstructDataDir(string? dataDir, string selfNodeId, string testScope, bool clean)
    {
        var dataRoot = PathKit.Combine(true, GetStableRoot(), GetType().Name, testScope, "cluster");
        if (clean && CleanedScopes.TryAdd(dataRoot, 0))
            DirectoryKit.TryDeleteDirectory(dataRoot);

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

    private PersistenceOptions GetPersistenceOptions(PersistenceOptions? persistenceOptions, string selfNodeId, string testScope, bool clean)
    {
        var dataDir = ConstructDataDir(persistenceOptions?.DataDir, selfNodeId, testScope, clean);
        return persistenceOptions ?? new PersistenceOptions
        {
            DataDir = dataDir,
            JournalMaxSegmentMb = 64,
            FlushIntervalMs = 10,
            SnapshotIntervalSec = 60,
        };
    }
}
