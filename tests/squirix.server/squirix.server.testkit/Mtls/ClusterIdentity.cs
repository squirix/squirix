using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Net.Http;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.IO;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Shared cluster CA and per-node mTLS material for multi-node test hosts in one test case.</summary>
[Mutable]
public sealed class ClusterIdentity : IDisposable
{
    private readonly Dictionary<string, HeldPort> _internalPorts = [with(StringComparer.Ordinal)];
    private readonly List<X509Certificate2> _ownedCertificates = [];
    private TestBundle? _bundle;
    private X509Certificate2? _untrustedCertificateAuthority;
    private int _disposed;

    /// <inheritdoc />
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
            return;

        foreach (var held in _internalPorts.Values)
            held.Dispose();

        for (var i = _ownedCertificates.Count - 1; i >= 0; i--)
            _ownedCertificates[i].Dispose();

        _ownedCertificates.Clear();
        _untrustedCertificateAuthority?.Dispose();
        _untrustedCertificateAuthority = null;
        _bundle?.Dispose();
        _bundle = null;
        _internalPorts.Clear();
    }

    /// <summary>Builds peer entries for a multi-node topology, including dedicated internode URLs.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <param name="identity">Shared context for the current test case.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    /// <exception cref="ArgumentException">Thrown when <paramref name="topology" /> is empty or contains an empty node identifier.</exception>
    internal static ServerPeer[] CreatePeers(ClusterNode[] topology, ref ClusterIdentity? identity)
    {
        if (topology.Length == 0)
            throw new ArgumentException("Topology must not be empty.", nameof(topology));

        for (var i = 0; i < topology.Length; i++)
        {
            if (string.IsNullOrWhiteSpace(topology[i].NodeId))
                throw new ArgumentException("Node identifiers must be non-empty.", nameof(topology));
        }

        if (!HasRemotePeers(topology))
        {
            var peers = new ServerPeer[topology.Length];
            for (var i = 0; i < topology.Length; i++)
                peers[i] = new ServerPeer { NodeId = topology[i].NodeId, Uri = topology[i].Uri };

            return peers;
        }

        identity ??= new ClusterIdentity();
        return identity.BuildPeers(topology);
    }

    /// <summary>Resolves startup mTLS material and releases the node's held internal port for immediate bind.</summary>
    /// <param name="identity">Shared identity for the current test case.</param>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    internal static async Task<(ClusterIdentity? Identity, MtlsOptions? Options, MtlsCertificate? Certificate)> ResolveForBindAsync(
        ClusterIdentity? identity,
        TopologyOptions cluster,
        CancellationToken cancellationToken = default)
    {
        var result = await ResolveForNodeAsync(identity, cluster, cancellationToken).ConfigureAwait(false);
        result.Identity?.ReleaseHeldInternalPort(cluster.NodeId);
        return result;
    }

    internal static async Task<(ClusterIdentity? Identity, MtlsOptions? Options, MtlsCertificate? Certificate)> ResolveForNodeAsync(
        ClusterIdentity? identity,
        TopologyOptions cluster,
        CancellationToken cancellationToken = default)
    {
        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return (identity, null, null);

        identity ??= new ClusterIdentity();
        var (options, material) = await identity.ResolveAsync(cluster, cancellationToken).ConfigureAwait(false);
        return (identity, options, material);
    }

    /// <summary>Returns a shared identity for the topology, creating one for multi-node topologies when none was supplied.</summary>
    /// <param name="topology">Cluster members that will be started from the shared identity.</param>
    /// <param name="identity">Caller-supplied shared identity, or <see langword="null" />.</param>
    /// <returns>The supplied identity; a new shared identity for multi-node topologies; otherwise <see langword="null" />.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="topology" /> is null.</exception>
    internal static ClusterIdentity? ResolveForTopology(ClusterNode[] topology, ClusterIdentity? identity)
    {
        ArgumentNullException.ThrowIfNull(topology);
        return identity ?? (HasRemotePeers(topology) ? new ClusterIdentity() : null);
    }

    /// <summary>Resolves startup overrides for a test node profile and releases its held internal port for immediate bind.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="profile">Requested internode mTLS test profile.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options, material, and optional per-peer outbound handler factory.</returns>
    internal async Task<NodeMtlsStartup> ResolveNodeStartupForBindAsync(TopologyOptions cluster, TestNodeProfile profile, CancellationToken cancellationToken = default)
    {
        var result = await ResolveNodeStartupAsync(cluster, profile, cancellationToken).ConfigureAwait(false);
        ReleaseHeldInternalPort(cluster.NodeId);
        return result;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(ClusterNode[] topology)
    {
        var ports = new HashSet<int>();
        for (var i = 0; i < topology.Length; i++)
            _ = ports.Add(topology[i].Uri.Port);

        return ports;
    }

    private static HashSet<int> CollectExcludedPrimaryPorts(TopologyOptions cluster)
    {
        var ports = new HashSet<int>();
        for (var i = 0; i < cluster.Peers.Count; i++)
            _ = ports.Add(cluster.Peers[i].Uri.Port);

        return ports;
    }

    private static Uri CreateInterNodeUrl(Uri primaryUrl, int internalPort) => new UriBuilder(primaryUrl.Scheme, primaryUrl.Host, internalPort).Uri;

    private static bool HasRemotePeers(ClusterNode[] topology)
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

    /// <summary>Builds peer entries with dedicated internode URLs, allocating held internal ports as needed.</summary>
    /// <param name="topology">Cluster members for peer configuration.</param>
    /// <returns>ServerPeer entries for host startup.</returns>
    /// <exception cref="ObjectDisposedException">Thrown when this identity has already been disposed.</exception>
    private ServerPeer[] BuildPeers(ClusterNode[] topology)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        var excludedPorts = CollectExcludedPrimaryPorts(topology);
        var peers = new ServerPeer[topology.Length];
        for (var i = 0; i < topology.Length; i++)
        {
            var node = topology[i];
            var internalPort = GetOrAllocateInternalPort(node.NodeId, excludedPorts).Port;
            peers[i] = new ServerPeer
            {
                NodeId = node.NodeId,
                Uri = node.Uri,
                InterNodeUri = CreateInterNodeUrl(node.Uri, internalPort),
            };
        }

        return peers;
    }

    private NodeMtlsStartup CreateExpiredPeerStartup(string nodeId, MtlsOptions options, MtlsCertificate material)
    {
        var clusterCa = _bundle!.GetClusterCertificateAuthority();
        var notBefore = new DateTimeOffset(clusterCa.NotBefore.AddHours(1).ToUniversalTime());
        var notAfter = DateTimeOffset.UtcNow.AddHours(-1);
        var expiredCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(clusterCa, nodeId, notBefore, notAfter));
        var clientCertificate = TrackCertificate(TestCertificates.LoadExportableCertificate(expiredCertificate));
        return new NodeMtlsStartup(options, material, new HandlerFactory(clientCertificate, material.TrustAnchor!).Create);
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Ownership of the created material transfers to the caller through the returned carrier and is disposed with the node host.")]
    private NodeMtlsStartup CreateUntrustedInboundServerStartup(string nodeId, MtlsOptions options, MtlsCertificate material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedServerCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        var serverCertificate = TrackCertificate(TestCertificates.LoadExportableCertificate(untrustedServerCertificate));
        var trustAnchor = material.TrustAnchor!;
        return new NodeMtlsStartup(options, MtlsCertificate.Create(serverCertificate, trustAnchor), null);
    }

    private NodeMtlsStartup CreateUntrustedOutboundStartup(string nodeId, MtlsOptions options, MtlsCertificate material)
    {
        var untrustedCa = GetOrCreateUntrustedCertificateAuthority();
        var untrustedClientCertificate = TrackCertificate(TestCertificates.CreatePeerCertificate(untrustedCa, nodeId));
        return new NodeMtlsStartup(options, material, new HandlerFactory(untrustedClientCertificate, material.TrustAnchor!).Create);
    }

    private HeldPort GetOrAllocateInternalPort(string nodeId, HashSet<int> excludedPorts)
    {
        if (_internalPorts.TryGetValue(nodeId, out var existing))
        {
            if (!existing.IsReleased)
                return existing;

            // A previous startup released the hold for its real bind, so the stored handle no longer
            // proves the port is bound. Reacquire a live hold for the same assigned port so a
            // restarted node's certificate generation stays protected. If the port is already
            // bound (for example by the live peer itself while another node starts), keep
            // advertising the assigned port — it is still the correct one.
            if (!excludedPorts.Contains(existing.Port))
            {
                var rehold = InternalPortPool.Rehold(existing.Port);
                if (rehold != null)
                    _internalPorts[nodeId] = rehold;

                return _internalPorts[nodeId];
            }

            _ = _internalPorts.Remove(nodeId);
        }

        // A fresh allocation must also avoid every other node's already-assigned internal port, but that
        // union is local to this allocation: excludedPorts may be the same set shared across every node in
        // BuildPeers' loop, and mutating it here would make an already-tracked node's own assigned port
        // exclude itself on a later iteration, forcing an unnecessary reallocation.
        var allocationExcluded = new HashSet<int>(excludedPorts);
        foreach (var held in _internalPorts.Values)
            _ = allocationExcluded.Add(held.Port);

        var allocated = InternalPortPool.AllocateInternalPort(allocationExcluded);
        _internalPorts[nodeId] = allocated;
        return allocated;
    }

    private X509Certificate2 GetOrCreateUntrustedCertificateAuthority() =>
        _untrustedCertificateAuthority ??= TrackCertificate(TestCertificates.CreateStandaloneCertificateAuthority());

    private void ReleaseHeldInternalPort(string nodeId)
    {
        if (_internalPorts.TryGetValue(nodeId, out var held))
            held.Dispose();
    }

    /// <summary>Creates cluster mTLS startup overrides for the node being started.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options and material for host startup overrides.</returns>
    private async Task<(MtlsOptions? Options, MtlsCertificate? Certificate)> ResolveAsync(TopologyOptions cluster, CancellationToken cancellationToken)
    {
        var (options, certificate, _) = await ResolveNodeStartupAsync(cluster, TestNodeProfile.Normal, cancellationToken).ConfigureAwait(false);
        return (options, certificate);
    }

    /// <summary>Resolves cluster mTLS startup overrides and outbound handler wiring for a test node profile.</summary>
    /// <param name="cluster">Cluster topology for the node.</param>
    /// <param name="profile">Requested internode mTLS test profile.</param>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>Options, material, and optional per-peer outbound handler factory.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="cluster" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="profile" /> is not supported.</exception>
    /// <exception cref="ObjectDisposedException">Thrown when this identity has already been disposed.</exception>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "MtlsCertificate created for the UntrustedInboundServer profile transfers to the caller through the returned carrier and is disposed with the node host.")]
    private async Task<NodeMtlsStartup> ResolveNodeStartupAsync(TopologyOptions cluster, TestNodeProfile profile, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        if (!MtlsTopology.RequiresInterNodeMtls(cluster))
            return new NodeMtlsStartup(null, null, null);

        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
        _bundle ??= new TestBundle();
        var port = GetOrAllocateInternalPort(cluster.NodeId, CollectExcludedPrimaryPorts(cluster)).Port;
        var (options, certificate) = await _bundle.CreateNodeAsync(cluster.NodeId, port, cancellationToken).ConfigureAwait(false);

        return profile switch
        {
            TestNodeProfile.Normal => new NodeMtlsStartup(options, certificate, null),
            TestNodeProfile.NoOutboundClientCertificate => new NodeMtlsStartup(options, certificate, new NoClientCertificateHandlerFactory(certificate.TrustAnchor!).Create),
            TestNodeProfile.UntrustedOutboundClientCertificate => CreateUntrustedOutboundStartup(cluster.NodeId, options, certificate),
            TestNodeProfile.UntrustedInboundServerCertificate => CreateUntrustedInboundServerStartup(cluster.NodeId, options, certificate),
            TestNodeProfile.ExpiredPeerCertificate => CreateExpiredPeerStartup(cluster.NodeId, options, certificate),
            _ => throw new ArgumentOutOfRangeException(nameof(profile), profile, "Unsupported mTLS test node profile."),
        };
    }

    private X509Certificate2 TrackCertificate(X509Certificate2 certificate)
    {
        _ownedCertificates.Add(certificate);
        return certificate;
    }

    private static class InternalPortPool
    {
        private static ListenPortPool Pool { get; } = ConsumerPortSlicer.PoolFor(HostPortRegion.MtlsInternal);

        /// <summary>Allocates a dedicated internal listener port that differs from all excluded primary ports.</summary>
        /// <param name="excludedPorts">Primary listener ports that must not be reused for internal mTLS.</param>
        /// <returns>A held internal listener port for cluster mTLS; disposing it releases the hold for the real bind.</returns>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="excludedPorts" /> is null.</exception>
        /// <exception cref="InvalidOperationException">Thrown if no internal listener port can be allocated within the attempt budget.</exception>
        /// <remarks>
        /// The port stays bound (with exclusive address use) until the caller releases it just before
        /// Kestrel binds, mirroring the primary-port discipline from #499. A bare probe-and-release
        /// leaves a TOCTOU window across certificate generation and sequential node startup where a
        /// parallel test can grab the same internal port (see #612).
        /// </remarks>
        internal static HeldPort AllocateInternalPort(HashSet<int> excludedPorts)
        {
            ArgumentNullException.ThrowIfNull(excludedPorts);

            for (var attempt = 0; attempt < 64; attempt++)
            {
                var held = Pool.HoldPort();
                if (!excludedPorts.Contains(held.Port))
                    return held;

                held.Dispose();
            }

            throw new InvalidOperationException("Failed to allocate a cluster mTLS internal listener port for tests.");
        }

        internal static HeldPort? Rehold(int port) => Pool.HoldSpecificPort(port);
    }

    [Immutable]
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

    [Immutable]
    private sealed class NoClientCertificateHandlerFactory
    {
        private readonly X509Certificate2 _trustAnchor;

        internal NoClientCertificateHandlerFactory(X509Certificate2 trustAnchor)
        {
            _trustAnchor = trustAnchor;
        }

        internal SocketsHttpHandler Create(string peerNodeId) => TestCertificates.CreateCaTrustingHandlerNoClientCert(_trustAnchor, peerNodeId);
    }

    /// <summary>Shared cluster CA and per-node mTLS material for multi-node integration and smoke tests.</summary>
    [Immutable]
    private sealed class TestBundle : IDisposable
    {
        private readonly X509Certificate2 _ca;
        private readonly TempDirectory _dir;

        /// <summary>Initializes a new instance of the <see cref="TestBundle" /> class.</summary>
        internal TestBundle()
        {
            _dir = new TempDirectory("squirix-cluster-mtls-cluster");
            _ca = CreateCertificateAuthority();
            FileKit.WriteAllText(GetClusterCertificateAuthorityPath(), _ca.ExportCertificatePem());
        }

        /// <inheritdoc />
        public void Dispose()
        {
            _ca.Dispose();
            _dir.Dispose();
        }

        /// <summary>Creates validated cluster mTLS options and loaded material for a test node.</summary>
        /// <param name="nodeId">Local node identifier.</param>
        /// <param name="port">Dedicated internal HTTPS listener port.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Options and material suitable for host startup overrides.</returns>
        internal async Task<(MtlsOptions Options, MtlsCertificate Material)> CreateNodeAsync(string nodeId, int port, CancellationToken cancellationToken = default)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(nodeId);
            PathValidationKit.ValidateSegmentName(nodeId, nameof(nodeId));

            var nodeDirectory = NodePathKit.Combine(_dir, nodeId);
            Directory.CreateDirectory(nodeDirectory);

            using var nodeCertificate = CreateNodeCertificate(nodeId);
            return await CreateNodeFromCertificateAsync(nodeId, port, nodeDirectory, nodeCertificate, cancellationToken).ConfigureAwait(false);
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

        private async Task<(MtlsOptions Options, MtlsCertificate Material)> CreateNodeFromCertificateAsync(
            string nodeId,
            int port,
            string nodeDirectory,
            X509Certificate2 nodeCertificate,
            CancellationToken cancellationToken)
        {
            var certificate = TestCertificates.LoadExportableCertificate(nodeCertificate);
            var path = NodePathKit.Combine(nodeDirectory, "node.pfx");
            await File.WriteAllBytesAsync(path, certificate.Export(X509ContentType.Pfx), cancellationToken).ConfigureAwait(false);

            var options = new MtlsOptions
            {
                CaPath = GetClusterCertificateAuthorityPath(),
                CertPfxPath = path,
                InternalListenPort = port,
            };

            var material = MtlsCertificate.Load(options, null, true, nodeId);
            return (options, material);
        }

        private string GetClusterCertificateAuthorityPath() => NodePathKit.Combine(_dir, "cluster-ca.crt");
    }
}
