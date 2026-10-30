using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Net.Client;
using JetBrains.Annotations;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>
/// Base class for all endpoint integration tests, providing helper methods to start test nodes,
/// manage test directories, and construct HTTP clients.
/// </summary>
public abstract class ServerSurfaceTestBase : IDisposable
{
    private readonly SocketsHttpHandler _socketsHttpHandler = LoopbackHttp.CreateHandler();
    private HttpClient? _httpClient;

    /// <summary>Gets a reusable <see cref="HttpClient" /> configured for gRPC/HTTP2 endpoint integration testing.</summary>
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
    internal static ValueTask<TestCluster<BlackBoxStartOptions>> StartClusterAsync(
        string nodeId,
        Func<string, BlackBoxStartOptions>? factory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync([new ClusterNode(nodeId, GetNextHttpUri())], factory, cancellationToken);

    /// <summary>Reserves one loopback listen URI per node and starts a two-node cluster.</summary>
    /// <param name="nodeA">First node identifier.</param>
    /// <param name="nodeB">Second node identifier.</param>
    /// <param name="optionsFactory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    internal static ValueTask<TestCluster<BlackBoxStartOptions>> StartClusterAsync(
        string nodeA,
        string nodeB,
        Func<string, BlackBoxStartOptions>? optionsFactory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync(
        [new ClusterNode(nodeA, GetNextHttpUri()), new ClusterNode(nodeB, GetNextHttpUri())],
        optionsFactory,
        cancellationToken);

    /// <summary>Starts a single-node cluster for the supplied topology entry.</summary>
    /// <param name="node">Node identifier paired with its listen URI.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the node.</returns>
    internal static ValueTask<TestCluster<BlackBoxStartOptions>> StartClusterAsync(
        ClusterNode node,
        Func<string, BlackBoxStartOptions>? factory = null,
        CancellationToken cancellationToken = default) => StartClusterAsync([node], factory, cancellationToken);

    /// <summary>Creates a gRPC channel configured for HTTPS against a test node URL.</summary>
    /// <param name="uri">The node listen URL.</param>
    /// <returns>A disposable gRPC channel.</returns>
    protected static GrpcChannel CreateGrpcChannel(Uri uri) => GrpcChannel.ForAddress(
        uri,
        new GrpcChannelOptions
        {
            HttpHandler = LoopbackHttp.CreateHandler(),
            DisposeHttpClient = true,
        });

    /// <summary>Disposes managed resources owned by the test base.</summary>
    /// <param name="disposing">True when called from <see cref="Dispose()" />; false from a finalizer path.</param>
    [UsedImplicitly]
    protected virtual void Dispose(bool disposing)
    {
        if (!disposing)
            return;

        _socketsHttpHandler.Dispose();
        _httpClient?.Dispose();
    }

    /// <summary>Allocates a unique loopback HTTPS listen URI, held bound until <see cref="BlackBoxCluster" /> releases it for the real bind.</summary>
    /// <returns>A loopback HTTPS listen URI.</returns>
    private static Uri GetNextHttpUri() => ListenPortPool.IntegrationTests.HoldHttpUri();

    /// <summary>Starts one node per topology entry, copying the entries so literal topologies do not allocate at the call site.</summary>
    /// <param name="topology">Node identifiers paired with their listen URIs, in start order.</param>
    /// <param name="factory">Optional per-node startup options keyed by node identifier.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started cluster owning the nodes.</returns>
    private static ValueTask<TestCluster<BlackBoxStartOptions>> StartClusterAsync(
        ReadOnlySpan<ClusterNode> topology,
        Func<string, BlackBoxStartOptions>? factory = null,
        CancellationToken cancellationToken = default)
    {
        var copy = new ClusterNode[topology.Length];
        topology.CopyTo(copy);
        return BlackBoxCluster.StartAsync(copy, factory, cancellationToken);
    }

    private HttpClient CreateHttpClient() => new(_socketsHttpHandler, false)
    {
        DefaultRequestVersion = HttpVersion.Version20,
        DefaultVersionPolicy = HttpVersionPolicy.RequestVersionOrHigher,
        Timeout = TimeSpan.FromSeconds(30),
    };
}
