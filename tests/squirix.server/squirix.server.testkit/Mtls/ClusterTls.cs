using System;
using System.Collections.Generic;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.</summary>
public sealed class ClusterTls : IDisposable
{
    private readonly Dictionary<string, int> _internalPortsByNodeId = new(StringComparer.Ordinal);
    private readonly List<X509Certificate2> _ownedCertificates = [];
    private TestBundle? _bundle;
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

    /// <summary>Builds a standalone single-peer topology without allocating a temporary topology span array.</summary>
    /// <param name="shared">Shared context for the current test case (unused for standalone peers).</param>
    /// <param name="nodeId">Local node identifier.</param>
    /// <param name="uri">Primary listen URL.</param>
    /// <returns>A one-element peer array.</returns>
    internal static ServerPeer[] CreatePeer(ref ClusterTls? shared, string nodeId, Uri uri)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
        ArgumentNullException.ThrowIfNull(uri);
        _ = shared;
        return [new ServerPeer { NodeId = nodeId, Uri = uri }];
    }

    /// <summary>Builds peer entries for a multi-node topology, including dedicated inter-node URLs.</summary>
    /// <param name="shared">Shared context for the current test case.</param>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topology" /> is empty.</exception>
    internal static ServerPeer[] CreatePeers(ref ClusterTls? shared, ReadOnlySpan<(string NodeId, Uri Uri)> topology)
    {
        if (topology.IsEmpty)
            throw new ArgumentException("Topology must not be empty.", nameof(topology));

        if (!HasRemotePeers(topology))
        {
            var standalonePeers = new ServerPeer[topology.Length];
            for (var i = 0; i < topology.Length; i++)
                standalonePeers[i] = new ServerPeer { NodeId = topology[i].NodeId, Uri = topology[i].Uri };

            return standalonePeers;
        }

        shared ??= new ClusterTls();
        return shared.BuildPeers(topology);
    }

    internal static async Task<(ClusterTls? Shared, MtlsOptions? Options, MtlsCertificateMaterial? Material)> ResolveForNodeAsync(
        ClusterTls? shared,
        TopologyOptions cluster,
        Uri uri,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(uri);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (shared, null, null);

        shared ??= new ClusterTls();
        var (options, material) = await shared.ResolveAsync(cluster, uri, cancellationToken).ConfigureAwait(false);
        return (shared, options, material);
    }

    /// <summary>Resolves cluster mTLS startup overrides and outbound handler wiring for a test node profile.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="uri">Primary listen URL for the node.</param>
    /// <param name="profile">Requested inter-node mTLS test profile.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options, material, and optional per-peer outbound handler factory.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="cluster" /> is null.</exception>
    /// <exception cref="ArgumentException">Thrown when <paramref name="uri" /> is null or whitespace.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="profile" /> is not supported.</exception>
    internal async Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory)> ResolveNodeStartupAsync(
        TopologyOptions cluster,
        Uri uri,
        TestNodeProfile profile,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cluster);
        ArgumentNullException.ThrowIfNull(uri);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (null, null, null);

        _bundle ??= new TestBundle();
        var internalPort = GetOrAllocateInternalPort(cluster.NodeId, cluster);
        var (options, material) = await _bundle.CreateNodeAsync(cluster.NodeId, internalPort, cancellationToken).ConfigureAwait(false);

        return profile switch
        {
            TestNodeProfile.Normal => (options, material, null),
            TestNodeProfile.NoOutboundClientCertificate => (options, material, new NoClientCertificateHandlerFactory(material.TrustAnchor!).Create),
            TestNodeProfile.UntrustedOutboundClientCertificate => CreateUntrustedOutboundStartup(cluster.NodeId, options, material),
            TestNodeProfile.UntrustedInboundServerCertificate => CreateUntrustedInboundServerStartup(cluster.NodeId, options, material),
            TestNodeProfile.ExpiredPeerCertificate => CreateExpiredPeerStartup(cluster.NodeId, options, material),
            _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported mTLS test node profile."),
        };
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(ReadOnlySpan<(string NodeId, Uri Uri)> topology)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < topology.Length; i++)
            _ = excludedPorts.Add(topology[i].Uri.Port);

        return excludedPorts;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(TopologyOptions cluster)
    {
        var excludedPorts = new HashSet<int>();
        for (var i = 0; i < cluster.Peers.Length; i++)
            _ = excludedPorts.Add(cluster.Peers[i].Uri.Port);

        return excludedPorts;
    }

    private static Uri CreateInterNodeUrl(Uri primaryUrl, int internalPort) => new UriBuilder(primaryUrl.Scheme, primaryUrl.Host, internalPort).Uri;

    private static bool HasRemotePeers(ReadOnlySpan<(string NodeId, Uri Uri)> topology)
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

    private ServerPeer[] BuildPeers(ReadOnlySpan<(string NodeId, Uri Uri)> topology)
    {
        var peers = new ServerPeer[topology.Length];
        for (var i = 0; i < topology.Length; i++)
        {
            var (nodeId, primaryUri) = topology[i];
            var internalPort = GetOrAllocateInternalPort(nodeId, topology);
            peers[i] = new ServerPeer
            {
                NodeId = nodeId,
                Uri = primaryUri,
                InterNodeUri = CreateInterNodeUrl(primaryUri, internalPort),
            };
        }

        return peers;
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory) CreateExpiredPeerStartup(
        string nodeId,
        MtlsOptions options,
        MtlsCertificateMaterial material)
    {
        var clusterCa = _bundle!.GetClusterCertificateAuthority();
        var notBefore = new DateTimeOffset(clusterCa.NotBefore.AddHours(1).ToUniversalTime());
        var notAfter = DateTimeOffset.UtcNow.AddHours(-1);
        var expiredCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(clusterCa, nodeId, notBefore, notAfter));
        var clientCertificate = TrackCertificate(TestCertificates.LoadExportableCertificate(expiredCertificate));
        return (options, material, new HandlerFactory(clientCertificate, material.TrustAnchor!).Create);
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory) CreateUntrustedInboundServerStartup(
        string nodeId,
        MtlsOptions options,
        MtlsCertificateMaterial material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedServerCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        var serverCertificate = TrackCertificate(TestCertificates.LoadExportableCertificate(untrustedServerCertificate));
        var trustAnchor = material.TrustAnchor!;
        return (options, MtlsCertificateMaterial.Create(serverCertificate, trustAnchor), null);
    }

    private (MtlsOptions? Options, MtlsCertificateMaterial? Material, Func<string, HttpMessageHandler>? PeerHandlerFactory) CreateUntrustedOutboundStartup(
        string nodeId,
        MtlsOptions options,
        MtlsCertificateMaterial material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedClientCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        return (options, material, new HandlerFactory(untrustedClientCertificate, material.TrustAnchor!).Create);
    }

    private int GetOrAllocateInternalPort(string nodeId, ReadOnlySpan<(string NodeId, Uri Uri)> topology)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var excludedPorts = CollectExcludedPrimaryPorts(topology);
        foreach (var allocatedPort in _internalPortsByNodeId.Values)
            _ = excludedPorts.Add(allocatedPort);

        var internalPort = InternalPortPool.AllocateInternalPort(excludedPorts);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }

    private int GetOrAllocateInternalPort(string nodeId, TopologyOptions cluster)
    {
        if (_internalPortsByNodeId.TryGetValue(nodeId, out var existingPort))
            return existingPort;

        var excludedPorts = CollectExcludedPrimaryPorts(cluster);
        foreach (var allocatedPort in _internalPortsByNodeId.Values)
            _ = excludedPorts.Add(allocatedPort);

        var internalPort = InternalPortPool.AllocateInternalPort(excludedPorts);
        _internalPortsByNodeId[nodeId] = internalPort;
        return internalPort;
    }

    private X509Certificate2 GetOrCreateUntrustedCertificateAuthority() =>
        _untrustedCertificateAuthority ??= TrackCertificate(TestCertificates.CreateStandaloneCertificateAuthority());

    /// <summary>Creates cluster mTLS startup overrides for the node being started.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="uri">Primary listen URL for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    private async Task<(MtlsOptions? Options, MtlsCertificateMaterial? Material)> ResolveAsync(TopologyOptions cluster, Uri uri, CancellationToken cancellationToken)
    {
        var (options, material, _) = await ResolveNodeStartupAsync(cluster, uri, TestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
        return (options, material);
    }

    private X509Certificate2 TrackCertificate(X509Certificate2 certificate)
    {
        _ownedCertificates.Add(certificate);
        return certificate;
    }

    private static class InternalPortPool
    {
        private static readonly PortAllocator Allocator = new(
            HostPortRegions.StartInclusive(HostPortRegion.MtlsInternal),
            HostPortRegions.EndExclusive(HostPortRegion.MtlsInternal) - 1);

        /// <summary>Allocates a dedicated internal listener port that differs from all excluded primary ports.</summary>
        /// <param name="excludedPorts">Primary listener ports that must not be reused for internal mTLS.</param>
        /// <returns>An internal listener port for cluster mTLS.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="excludedPorts" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if no internal listener port can be allocated within the attempt budget.</exception>
        internal static int AllocateInternalPort(HashSet<int> excludedPorts)
        {
            ArgumentNullException.ThrowIfNull(excludedPorts);

            for (var attempt = 0; attempt < 64; attempt++)
            {
                var port = Allocator.Allocate();
                var isExcluded = false;
                foreach (var excludedPort in excludedPorts)
                {
                    if (excludedPort != port)
                        continue;
                    isExcluded = true;
                    break;
                }

                if (!isExcluded)
                    return port;
            }

            throw new InvalidOperationException("Failed to allocate a cluster mTLS internal listener port for tests.");
        }
    }

    private sealed class HandlerFactory
    {
        private readonly X509CertificateCollection _clientCertificates;
        private readonly X509Certificate2 _trustAnchor;

        internal HandlerFactory(X509Certificate2 clientCertificate, X509Certificate2 trustAnchor)
        {
            _clientCertificates = [clientCertificate];
            _trustAnchor = trustAnchor;
        }

        internal SocketsHttpHandler Create(string peerNodeId) => TestCertificates.CreateMtlsHandler(_clientCertificates, _trustAnchor, peerNodeId);
    }

    private sealed class NoClientCertificateHandlerFactory
    {
        private readonly X509Certificate2 _trustAnchor;

        internal NoClientCertificateHandlerFactory(X509Certificate2 trustAnchor)
        {
            _trustAnchor = trustAnchor;
        }

        internal SocketsHttpHandler Create(string peerNodeId) => TestCertificates.CreateClusterCaTrustingHandlerNoClientCert(_trustAnchor, peerNodeId);
    }

    /// <summary>Shared cluster CA and per-node mTLS material for multi-node integration and smoke tests.</summary>
    private sealed class TestBundle : IDisposable
    {
        private readonly X509Certificate2 _ca;
        private readonly TempDirectory _rootDirectory;

        /// <summary>
        /// Initializes a new instance of the <see cref="TestBundle" /> class.
        /// </summary>
        internal TestBundle()
        {
            _rootDirectory = new TempDirectory("squirix-cluster-mtls-cluster");
            _ca = CreateCertificateAuthority();
            FileKit.WriteAllText(GetClusterCertificateAuthorityPath(), _ca.ExportCertificatePem());
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _ca.Dispose();
            _rootDirectory.Dispose();
        }

        /// <summary>Creates validated cluster mTLS options and loaded material for a test node.</summary>
        /// <param name="nodeId">Local node identifier.</param>
        /// <param name="internalListenPort">Dedicated internal HTTPS listener port.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Options and material suitable for host startup overrides.</returns>
        internal async Task<(MtlsOptions Options, MtlsCertificateMaterial Material)> CreateNodeAsync(
            string nodeId,
            int internalListenPort,
            CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);

            var nodeDirectory = NodePathKit.Combine(_rootDirectory, nodeId);
            DirectoryKit.CreateDirectory(nodeDirectory);

            using var nodeCertificate = CreateNodeCertificate(nodeId);
            return await CreateNodeFromCertificateAsync(nodeId, internalListenPort, nodeDirectory, nodeCertificate, cancellationToken).ConfigureAwait(false);
        }

        internal X509Certificate2 GetClusterCertificateAuthority() => _ca;

        private static X509Certificate2 CreateCertificateAuthority()
        {
            using var caKey = RSA.Create(2048);
            var caRequest = new CertificateRequest("CN=Squirix Cluster Test CA", caKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            caRequest.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
            var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
            var notAfter = notBefore.AddDays(30);
            return caRequest.CreateSelfSigned(notBefore, notAfter);
        }

        private X509Certificate2 CreateNodeCertificate(string nodeId)
        {
            using var nodeKey = RSA.Create(2048);
            var nodeRequest = new CertificateRequest($"CN={nodeId}", nodeKey, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
            nodeRequest.AddClusterNodeExtensions();
            var nodePublic = nodeRequest.Create(
                _ca,
                new DateTimeOffset(_ca.NotBefore.ToUniversalTime()),
                new DateTimeOffset(_ca.NotAfter.ToUniversalTime()),
                Guid.NewGuid().ToByteArray());
            return nodePublic.HasPrivateKey ? nodePublic : nodePublic.CopyWithPrivateKey(nodeKey);
        }

        private async Task<(MtlsOptions Options, MtlsCertificateMaterial Material)> CreateNodeFromCertificateAsync(
            string nodeId,
            int internalListenPort,
            string nodeDirectory,
            X509Certificate2 nodeCertificate,
            CancellationToken cancellationToken)
        {
            _ = nodeId;
            var exportableCertificate = TestCertificates.LoadExportableCertificate(nodeCertificate);
            var pfxPath = NodePathKit.Combine(nodeDirectory, "node.pfx");
            await File.WriteAllBytesAsync(pfxPath, exportableCertificate.Export(X509ContentType.Pfx), cancellationToken).ConfigureAwait(false);

            var options = new MtlsOptions
            {
                CaPath = GetClusterCertificateAuthorityPath(),
                CertPfxPath = pfxPath,
                InternalListenPort = internalListenPort,
            };

            var material = MtlsCertificateMaterial.Load(options, null, true, nodeId);
            return (options, material);
        }

        private string GetClusterCertificateAuthorityPath() => NodePathKit.Combine(_rootDirectory, "cluster-ca.crt");
    }
}
