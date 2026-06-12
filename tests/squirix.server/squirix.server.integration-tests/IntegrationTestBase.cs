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
using Squirix.Server.Runtime;
using Squirix.Server.Runtime.Contracts;
using Squirix.Server.Storage;
using Squirix.Server.Storage.Snapshot;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Cluster;
using Squirix.Server.TestKit.Http;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.XUnit;
using Xunit;

namespace Squirix.Server.IntegrationTests;

/// <summary>
/// Base class for squirix integration tests.
/// Provides helpers for starting nodes, building entries,
/// and creating test-scoped persistence directories.
/// </summary>
public abstract class IntegrationTestBase : IDisposable
{
    private static readonly ConcurrentDictionary<string, byte> CleanedScopes = new();
    private static readonly PortAllocator PortPool = CreatePortAllocator();
    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();

    private MtlsTestContext? _mtls;
    private HttpClient? _httpClient;

    static IntegrationTestBase()
    {
        Environment.SetEnvironmentVariable("SQUIRIX_TEST_ROOT", PathKit.GetProcTempPath());
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

    /// <summary>
    /// Cleans up sockets handler, HTTP client, and cancellation tokens.
    /// </summary>
    public virtual void Dispose()
    {
        _mtls?.Dispose();
        _socketsHttpHandler.Dispose();
        _httpClient?.Dispose();
        GC.SuppressFinalize(this);
    }

    /// <summary>
    /// Convenience builder for a <see cref="CacheEntry{T}" /> with optional expiration, version, and tags.
    /// </summary>
    /// <param name="value">
    /// The value to store. If a <see cref="JsonDocument" /> or <see cref="JsonElement" /> is supplied,
    /// it is cloned to detach from the underlying document’s lifetime; otherwise the value is used as-is.
    /// </param>
    /// <param name="expiresUtc">
    /// Optional absolute UTC expiration time. When <c>null</c>, the entry does not have an absolute expiry.
    /// </param>
    /// <param name="version">
    /// The initial monotonic version to assign to the entry. Defaults to <c>1</c>.
    /// </param>
    /// <param name="tags">
    /// Optional set of user-defined tags. When provided, the collection is frozen using an ordinal string comparer.
    /// </param>
    /// <returns>
    /// A new <see cref="CacheEntry{T}" /> instance with the provided <paramref name="value" />, <paramref name="expiresUtc" />,
    /// <paramref name="version" />, and <paramref name="tags" />; <c>Expiration</c> is set to <c>null</c>.
    /// </returns>
    internal static CacheEntry<object?> BuildEntry(object? value, DateTime? expiresUtc = null, long version = 1, IDictionary<string, string>? tags = null)
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

    /// <summary>
    /// Resolves the cluster-aware cache API client from the test node’s dependency injection container.
    /// </summary>
    /// <param name="host">The started test node host providing access to the service provider.</param>
    /// <returns>The resolved <see cref="ICacheApi{T}" /> instance.</returns>
    /// <exception cref="InvalidOperationException">
    /// Thrown if <see cref="ICacheApi{T}" /> is not registered in the node’s service provider.
    /// </exception>
    internal static ILogicalNamespacedCache<object?> GetCache(TestNodeHost host) => host.Services.GetRequiredService<ICacheRuntime>().GetCache<object?>("default");

    /// <summary>
    /// Starts a new <see cref="SquirixNodeHost" /> for integration testing with configurable peers,
    /// persistence, gRPC configuration, and extra services.
    /// </summary>
    /// <param name="url">
    /// The node’s listen URL (HTTP or HTTPS). Must correspond to one of the <paramref name="peers" /> entries.
    /// </param>
    /// <param name="peers">
    /// The cluster peer set, including the node being started (its <see cref="Peer.Url" /> must equal <paramref name="url" />).
    /// </param>
    /// <param name="callPolicyFactory">
    /// Optional factory used to create a <see cref="CallPolicy" /> for outbound peer calls. If <c>null</c>, a default policy is used.
    /// The factory receives the peer URL and should return a configured policy instance.
    /// </param>
    /// <param name="configureGrpc">
    /// Optional callback to configure <see cref="GrpcServiceOptions" />.
    /// </param>
    /// <param name="servicesConfigure">
    /// Optional callback to register/override services in the node’s DI container (e.g., test doubles, exporters).
    /// </param>
    /// <param name="snapshotOptions">
    /// Optional snapshot trigger options; when <c>null</c>, the node uses its built-in defaults.
    /// </param>
    /// <param name="persistenceOptions">
    /// Optional base persistence options. The data directory is overridden per test (node id + scope);
    /// other fields are honored as provided.
    /// </param>
    /// <param name="usePersistence">
    /// When <c>true</c>, starts the node with WAL/snapshot persistence enabled using a test-scoped data directory.
    /// </param>
    /// <param name="output">
    /// Optional xUnit output helper. When provided, logs are routed to xUnit; otherwise Console/Debug loggers are used.
    /// </param>
    /// <param name="cleanTestDir">
    /// If <c>true</c>, the per-test data directory is cleaned before startup. If <c>false</c>, it is reused.
    /// </param>
    /// <param name="extraScope">
    /// Optional additional path segment appended to the test scope to isolate data directories between logical scenarios.
    /// </param>
    /// <param name="httpHandlerOverride">
    /// Optional HTTP message handler used by the ClientPool for outbound gRPC calls in tests (enables chaos/fault injection).
    /// </param>
    /// <param name="backpressureOptions">
    /// Optional backpressure options for inbound admission control.
    /// </param>
    /// <param name="runtimeOptions">
    /// Optional cache runtime options such as strict type binding policy.
    /// </param>
    /// <param name="memoryPressureOptions">
    /// Optional memory pressure options; when <c>null</c>, the host loads defaults merged from <c>Squirix.settings.json</c> and environment variables.
    /// </param>
    /// <param name="security">
    /// Optional per-node security override. When set, environment variables are not read for auth on this startup.
    /// </param>
    /// <param name="testName">
    /// Optional scope hint from the caller (often via <see cref="CallerMemberNameAttribute" />).
    /// Under xUnit, <see cref="TestPersistenceScope.ResolvePersistenceScopeSegment" /> uses the active test case id when available.
    /// </param>
    /// <returns>
    /// A started <see cref="TestNodeHost" /> wrapper containing the running application, its base URL, and the resolved data directory.
    /// Dispose it to stop the node and release resources.
    /// </returns>
    /// <exception cref="ArgumentException">
    /// Thrown when <paramref name="peers" /> does not contain an entry for <paramref name="url" /> (the self node).
    /// </exception>
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
        HttpMessageHandler? httpHandlerOverride = null,
        BackpressureOptions? backpressureOptions = null,
        CacheRuntimeOptions? runtimeOptions = null,
        MemoryPressureOptions? memoryPressureOptions = null,
        TestNodeSecurityOptions? security = null,
        [CallerMemberName] string? testName = null)
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

        var scopeName = TestPersistenceScope.ResolvePersistenceScopeSegment(testName);
        PersistenceOptions? persistenceOptionsOverride = null;
        var dataDir = string.Empty;
        if (usePersistence || persistenceOptions is not null)
        {
            persistenceOptionsOverride = GetPersistenceOptions(persistenceOptions, selfNodeId, BuildTestScope(scopeName, extraScope), cleanTestDir);
            dataDir = persistenceOptionsOverride.DataDir;
        }

        var (mtlsOptions, mtlsMaterial) = MtlsTestContext.ResolveForNode(ref _mtls, clusterConfig, url);

        var application = await SquirixNodeHost.StartAsync(
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
            httpHandlerOverride ?? LoopbackHttp.CreateHandler(),
            backpressureOptions,
            runtimeOptions,
            memoryPressureOptions,
            security?.ToServerOptions(),
            null,
            mtlsOptions,
            mtlsMaterial,
            DefaultCancellationToken);

        return new TestNodeHost(application, url, dataDir, persistenceOptionsOverride is not null);
    }

    /// <summary>
    /// Allocates a dedicated port reserved for the lifetime of the test process.
    /// </summary>
    /// <returns>A port number reserved from the shared in-process pool.</returns>
    protected static int AllocateDedicatedPort() => PortPool.Allocate();

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
    /// Allocates a unique HTTP URL for the next node using the shared port pool.
    /// </summary>
    /// <returns>
    /// A loopback HTTPS URL of the form <c>https://127.0.0.1:&lt;port&gt;</c>, where <c>&lt;port&gt;</c>
    /// is a free port reserved from the shared pool.
    /// </returns>
    protected static string GetNextHttpUrl() => $"https://127.0.0.1:{PortPool.Allocate()}";

    private static string BuildTestScope(string? testName, string? extra)
    {
        var baseName = string.IsNullOrWhiteSpace(testName) ? "unknown" : testName;
        var scope = string.IsNullOrWhiteSpace(extra) ? baseName : $"{baseName}__{extra}";

        var tfm = AppContext.TargetFrameworkName;
        if (!string.IsNullOrWhiteSpace(tfm))
            scope = $"{scope}__{tfm}";

        return $"{scope}__pid{Environment.ProcessId}";
    }

    private static PortAllocator CreatePortAllocator()
    {
        const int start = 49000;
        const int rangeSize = 4000;
        var end = Math.Min(65535, start + rangeSize - 1);
        return new PortAllocator(start, end);
    }

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };

    private PersistenceOptions GetPersistenceOptions(PersistenceOptions? persistenceOptions, string selfNodeId, string testScope, bool clean)
    {
        var path = PathKit.Combine(true, PathKit.GetProcTempPath(), GetType().Name, testScope, "cluster");
        if (clean && CleanedScopes.TryAdd(path, 0))
            DirectoryKit.TryDeleteDirectory(path);

        var effectiveDataDir = persistenceOptions?.DataDir ?? PathKit.Combine(true, path, selfNodeId);
        DirectoryKit.CreateDirectory(effectiveDataDir);

        return persistenceOptions ?? new PersistenceOptions
        {
            DataDir = effectiveDataDir,
            JournalMaxSegmentMb = 64,
        };
    }
}
