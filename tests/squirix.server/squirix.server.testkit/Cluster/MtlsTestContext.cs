using System;
using System.Collections.Generic;
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.Cluster.Transport;

namespace Squirix.Server.TestKit.Cluster;

/// <summary>
/// Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.
/// </summary>
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

    /// <summary>
    /// Builds peer entries for a multi-node topology, including dedicated inter-node URLs.
    /// </summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>Peer entries for host startup.</returns>
    internal static Peer[] CreatePeers(ref MtlsTestContext? shared, IReadOnlyList<(string NodeId, string Url)> topology)
    {
        ArgumentNullException.ThrowIfNull(topology);

        if (!HasRemotePeers(topology))
        {
            var standalonePeers = new Peer[topology.Count];
            for (var i = 0; i < topology.Count; i++)
                standalonePeers[i] = new Peer { NodeId = topology[i].NodeId, Url = topology[i].Url };

            return standalonePeers;
        }

        shared ??= new MtlsTestContext();
        return shared.BuildPeers(topology);
    }

    /// <summary>
    /// Resolves startup overrides for a node, reusing one shared context per test case.
    /// </summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Startup overrides, or <c>null</c> values for standalone topology.</returns>
    internal static (MtlsOptions? Options, MtlsCertificateMaterial? Material) ResolveForNode(ref MtlsTestContext? shared, ClusterConfig cluster, string url)
    {
        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null);

        shared ??= new MtlsTestContext();
        return shared.Resolve(cluster, url);
    }

    /// <summary>
    /// Resolves cluster mTLS startup overrides and outbound handler wiring for a test node profile.
    /// </summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <param name="profile">Requested inter-node mTLS test profile.</param>
    /// <returns>Options, material, and optional outbound handler override.</returns>
    internal (MtlsOptions? Options, MtlsCertificateMaterial? Material, HttpMessageHandler? ClusterHttpHandler) ResolveNodeStartup(
        ClusterConfig cluster,
        string url,
        MtlsTestNodeProfile profile)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentException.ThrowIfNullOrWhiteSpace(url);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null, null);

        _bundle ??= new MtlsTestBundle();
        var internalPort = GetOrAllocateInternalPort(cluster.NodeId, cluster);
        var (options, material) = _bundle.CreateNode(cluster.NodeId, internalPort);

        return profile switch
        {
            MtlsTestNodeProfile.Normal => (options, material, GrpcTransportEndpoints.CreateMtlsHandler(material)),
            MtlsTestNodeProfile.NoOutboundClientCertificate => (options, material,
                MtlsTestCertificates.CreateClusterCaTrustingHandlerWithoutClientCertificate(material.TrustAnchor!)),
            MtlsTestNodeProfile.UntrustedOutboundClientCertificate => CreateUntrustedOutboundStartup(cluster.NodeId, options, material),
            MtlsTestNodeProfile.UntrustedInboundServerCertificate => CreateUntrustedInboundServerStartup(cluster.NodeId, internalPort, material),
            MtlsTestNodeProfile.ExpiredPeerCertificate => CreateExpiredPeerStartup(cluster.NodeId, internalPort, material),
            _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported mTLS test node profile."),
        };
    }

    private static string CreateInterNodeUrl(string primaryUrl, int internalPort)
    {
        var primaryUri = new Uri(primaryUrl);
        return new UriBuilder(primaryUri.Scheme, primaryUri.Host, internalPort).Uri.AbsoluteUri;
    }

    private static bool HasRemotePeers(IReadOnlyList<(string NodeId, string Url)> topology)
    {
        var nodeIds = new HashSet<string>(StringComparer.Ordinal);
        for (var i = 0; i < topology.Count; i++)
            _ = nodeIds.Add(topology[i].NodeId);

        return nodeIds.Count > 1;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(IReadOnlyList<(string NodeId, string Url)> topology)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < topology.Count; i++)
            _ = excludedPorts.Add(new Uri(topology[i].Url).Port);

        return excludedPorts;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(ClusterConfig cluster)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < cluster.Peers.Length; i++)
        {
            if (Uri.TryCreate(cluster.Peers[i].Url, UriKind.Absolute, out var peerUri))
                _ = excludedPorts.Add(peerUri.Port);
        }

        return excludedPorts;
    }

    private Peer[] BuildPeers(IReadOnlyList<(string NodeId, string Url)> topology)
    {
        var peers = new Peer[topology.Count];
        for (var i = 0; i < topology.Count; i++)
        {
            var (nodeId, url) = topology[i];
            var internalPort = GetOrAllocateInternalPort(nodeId, topology);
            peers[i] = new Peer
            {
                NodeId = nodeId,
                Url = url,
                InterNodeUrl = CreateInterNodeUrl(url, internalPort),
            };
        }

        return peers;
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, HttpMessageHandler? ClusterHttpHandler) CreateExpiredPeerStartup(
        string nodeId,
        int internalListenPort,
        MtlsCertificateMaterial material)
    {
        var clusterCa = _bundle!.GetClusterCertificateAuthority();
        var notBefore = clusterCa.NotBefore.AddHours(1);
        var notAfter = DateTimeOffset.UtcNow.AddHours(-1);
        using var expiredCertificate = MtlsTestCertificates.CreatePeerCertificate(clusterCa, nodeId, notBefore, notAfter);
        return CreateMaterialStartup(nodeId, internalListenPort, expiredCertificate, material.TrustAnchor!);
    }

    private (MtlsOptions Options, MtlsCertificateMaterial Material, HttpMessageHandler ClusterHttpHandler) CreateMaterialStartup(
        string nodeId,
        int internalListenPort,
        X509Certificate2 nodeCertificate,
        X509Certificate2 trustAnchor)
    {
        _ = trustAnchor;
        var (options, material) = _bundle!.CreateNodeFromCertificate(nodeId, internalListenPort, nodeCertificate);
        return (options, material, GrpcTransportEndpoints.CreateMtlsHandler(material));
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, HttpMessageHandler? ClusterHttpHandler) CreateUntrustedInboundServerStartup(
        string nodeId,
        int internalListenPort,
        MtlsCertificateMaterial material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        using var untrustedServerCertificate = MtlsTestCertificates.CreatePeerCertificate(untrustedCa, nodeId);
        return CreateMaterialStartup(nodeId, internalListenPort, untrustedServerCertificate, material.TrustAnchor!);
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, HttpMessageHandler? ClusterHttpHandler) CreateUntrustedOutboundStartup(
        string nodeId,
        MtlsOptions options,
        MtlsCertificateMaterial material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedClientCertificate = TrackCertificate(MtlsTestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        var outboundMaterial = MtlsCertificateMaterial.FromCertificates(untrustedClientCertificate, material.TrustAnchor!);
        return (options, material, GrpcTransportEndpoints.CreateMtlsHandler(outboundMaterial));
    }

    private int GetOrAllocateInternalPort(string nodeId, IReadOnlyList<(string NodeId, string Url)> topology)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var excludedPorts = CollectExcludedPrimaryPorts(topology);
        foreach (var allocatedPort in _internalPortsByNodeId.Values)
            _ = excludedPorts.Add(allocatedPort);

        var internalPort = MtlsTestPorts.AllocateInternalPort(excludedPorts);
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

        var internalPort = MtlsTestPorts.AllocateInternalPort(excludedPorts);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }

    private X509Certificate2 GetOrCreateUntrustedCertificateAuthority() =>
        _untrustedCertificateAuthority ??= TrackCertificate(MtlsTestCertificates.CreateStandaloneCertificateAuthority());

    /// <summary>
    /// Creates cluster mTLS startup overrides for the node being started.
    /// </summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="url">Primary listen URL for the node.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    private (MtlsOptions? Options, MtlsCertificateMaterial? Material) Resolve(ClusterConfig cluster, string url)
    {
        var (options, material, _) = ResolveNodeStartup(cluster, url, MtlsTestNodeProfile.Normal);
        return (options, material);
    }

    private X509Certificate2 TrackCertificate(X509Certificate2 certificate)
    {
        _ownedCertificates.Add(certificate);
        return certificate;
    }
}
