using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.</summary>
public sealed class MtlsTestContext : IDisposable
{
    private readonly Dictionary<string, int> _internalPortsByNodeId = new(StringComparer.Ordinal);
    private readonly List<X509Certificate2> _ownedCertificates = [];
    private MtlsTestBundle? _bundle;
    private X509Certificate2? _untrustedCertificateAuthority;

    /// <inheritdoc />
    public void Dispose()
    {
        for (var i = _ownedCertificates.Count - 1; i >= 0; i--)
            _ownedCertificates[i].Dispose();

        _ownedCertificates.Clear();
        _untrustedCertificateAuthority?.Dispose();
        _untrustedCertificateAuthority = null;
        _bundle?.Dispose();
        _bundle = null;
        _internalPortsByNodeId.Clear();
    }

    /// <summary>Builds peer entries for a multi-node topology, including dedicated inter-node URLs.</summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>Peer entries for host startup.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topology" /> is empty.</exception>
    internal static Peer[] CreatePeers(ref MtlsTestContext? shared, ReadOnlySpan<(string NodeId, string Url)> topology)
    {
        if (topology.IsEmpty)
            throw new ArgumentException("Topology must not be empty.", nameof(topology));

        if (!HasRemotePeers(topology))
        {
            var standalonePeers = new Peer[topology.Length];
            for (var i = 0; i < topology.Length; i++)
                standalonePeers[i] = new Peer { NodeId = topology[i].NodeId, Url = new Uri(topology[i].Url, UriKind.Absolute) };

            return standalonePeers;
        }

        shared ??= new MtlsTestContext();
        return shared.BuildPeers(topology);
    }

    internal static async Task<(MtlsTestContext? Shared, MtlsOptions? Options, MtlsCertificateMaterial? Material)> ResolveForNodeAsync(
        MtlsTestContext? shared,
        ClusterConfig cluster,
        Uri url,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(url);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (shared, null, null);

        shared ??= new MtlsTestContext();
        var (options, material) = await shared.ResolveAsync(cluster, url, cancellationToken).ConfigureAwait(false);
        return (shared, options, material);
    }

    /// <summary>Resolves cluster mTLS startup overrides and outbound handler wiring for a test node profile.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <param name="profile">Requested inter-node mTLS test profile.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options, material, and optional per-peer outbound handler factory.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="cluster" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="url" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="profile" /> is not supported.</exception>
    internal async Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory)> ResolveNodeStartupAsync(
        ClusterConfig cluster,
        Uri url,
        MtlsTestNodeProfile profile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(url);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null, null);

        _bundle ??= new MtlsTestBundle();
        var internalPort = GetOrAllocateInternalPort(cluster.NodeId, cluster);
        var (options, material) = await _bundle.CreateNodeAsync(cluster.NodeId, internalPort, cancellationToken).ConfigureAwait(false);

        return profile switch
        {
            MtlsTestNodeProfile.Normal => (options, material, null),
            MtlsTestNodeProfile.NoOutboundClientCertificate => (options, material,
                peerNodeId => MtlsTestCertificates.CreateClusterCaTrustingHandlerWithoutClientCertificate(material.TrustAnchor!, peerNodeId)),
            MtlsTestNodeProfile.UntrustedOutboundClientCertificate => CreateUntrustedOutboundStartup(cluster.NodeId, options, material),
            MtlsTestNodeProfile.UntrustedInboundServerCertificate => await CreateUntrustedInboundServerStartupAsync(cluster.NodeId, internalPort, material, cancellationToken).ConfigureAwait(false),
            MtlsTestNodeProfile.ExpiredPeerCertificate => await CreateExpiredPeerStartupAsync(cluster.NodeId, internalPort, material, cancellationToken).ConfigureAwait(false),
            _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported mTLS test node profile."),
        };
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(ReadOnlySpan<(string NodeId, string Url)> topology)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < topology.Length; i++)
            _ = excludedPorts.Add(new Uri(topology[i].Url).Port);

        return excludedPorts;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(ClusterConfig cluster)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < cluster.Peers.Length; i++)
            _ = excludedPorts.Add(cluster.Peers[i].Url.Port);

        return excludedPorts;
    }

    private static Uri CreateInterNodeUrl(Uri primaryUrl, int internalPort) =>
        new UriBuilder(primaryUrl.Scheme, primaryUrl.Host, internalPort).Uri;

    private static bool HasRemotePeers(ReadOnlySpan<(string NodeId, string Url)> topology)
    {
        if (topology.Length <= 1)
            return false;

        var firstNodeId = topology[0].NodeId;
        for (var i = 1; i < topology.Length; i++)
        {
            if (!string.Equals(topology[i].NodeId, firstNodeId, StringComparison.Ordinal))
                return true;
        }

        return false;
    }

    private Peer[] BuildPeers(ReadOnlySpan<(string NodeId, string Url)> topology)
    {
        var peers = new Peer[topology.Length];
        for (var i = 0; i < topology.Length; i++)
        {
            var (nodeId, url) = topology[i];
            var primaryUrl = new Uri(url, UriKind.Absolute);
            var internalPort = GetOrAllocateInternalPort(nodeId, topology);
            peers[i] = new Peer
            {
                NodeId = nodeId,
                Url = primaryUrl,
                InterNodeUrl = CreateInterNodeUrl(primaryUrl, internalPort),
            };
        }

        return peers;
    }

    private Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory)> CreateExpiredPeerStartupAsync(
        string nodeId,
        int internalListenPort,
        MtlsCertificateMaterial material,
        CancellationToken cancellationToken)
    {
        var clusterCa = _bundle!.GetClusterCertificateAuthority();
        var notBefore = new DateTimeOffset(clusterCa.NotBefore.AddHours(1).ToUniversalTime());
        var notAfter = DateTimeOffset.UtcNow.AddHours(-1);
        var expiredCertificate = TrackCertificate(MtlsTestCertificates.CreatePeerCertificate(clusterCa, nodeId, notBefore, notAfter));
        return CreateMaterialStartupAsync(nodeId, internalListenPort, expiredCertificate, material.TrustAnchor!, cancellationToken);
    }

    private async Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory)> CreateMaterialStartupAsync(
        string nodeId,
        int internalListenPort,
        X509Certificate2 nodeCertificate,
        X509Certificate2 trustAnchor,
        CancellationToken cancellationToken)
    {
        var (options, material) = await _bundle!.CreateNodeFromCertificateAsync(nodeId, internalListenPort, nodeCertificate, cancellationToken).ConfigureAwait(false);
        var clientCertificate = TrackCertificate(MtlsTestCertificates.LoadExportableCertificate(nodeCertificate));
        return (options, material, peerNodeId => GrpcTransportEndpoints.CreateMtlsHandler(clientCertificate, trustAnchor, peerNodeId));
    }

    private Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory)> CreateUntrustedInboundServerStartupAsync(
        string nodeId,
        int internalListenPort,
        MtlsCertificateMaterial material,
        CancellationToken cancellationToken)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedServerCertificate = TrackCertificate(MtlsTestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        return CreateMaterialStartupAsync(nodeId, internalListenPort, untrustedServerCertificate, material.TrustAnchor!, cancellationToken);
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory) CreateUntrustedOutboundStartup(
        string nodeId,
        MtlsOptions options,
        MtlsCertificateMaterial material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedClientCertificate = TrackCertificate(MtlsTestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        var trustAnchor = material.TrustAnchor!;
        return (options, material, peerNodeId => GrpcTransportEndpoints.CreateMtlsHandler(untrustedClientCertificate, trustAnchor, peerNodeId));
    }

    private int GetOrAllocateInternalPort(string nodeId, ReadOnlySpan<(string NodeId, string Url)> topology)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var excludedPorts = CollectExcludedPrimaryPorts(topology);
        foreach (var allocatedPort in _internalPortsByNodeId.Values)
            _ = excludedPorts.Add(allocatedPort);

        var internalPort = MtlsInternalPortPool.AllocateInternalPort(excludedPorts);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }

    private int GetOrAllocateInternalPort(string nodeId, ClusterConfig cluster)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var excludedPorts = CollectExcludedPrimaryPorts(cluster);
        foreach (var allocatedPort in _internalPortsByNodeId.Values)
            _ = excludedPorts.Add(allocatedPort);

        var internalPort = MtlsInternalPortPool.AllocateInternalPort(excludedPorts);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }

    private X509Certificate2 GetOrCreateUntrustedCertificateAuthority() =>
        _untrustedCertificateAuthority ??= TrackCertificate(MtlsTestCertificates.CreateStandaloneCertificateAuthority());

    /// <summary>Creates cluster mTLS startup overrides for the node being started.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    private async Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material)> ResolveAsync(
        ClusterConfig cluster,
        Uri url,
        CancellationToken cancellationToken)
    {
        var (options, material, _) = await ResolveNodeStartupAsync(cluster, url, MtlsTestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
        return (options, material);
    }

    private X509Certificate2 TrackCertificate(X509Certificate2 certificate)
    {
        _ownedCertificates.Add(certificate);
        return certificate;
    }
}
