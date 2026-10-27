using System;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using Squirix.Server.Attributes;

namespace Squirix.Server.TestKit.Mtls;

/// <summary>Process-wide cluster CA and per-node mTLS certificates, created once and handed out as independent copies.</summary>
/// <remarks>
/// Key generation and persisted-key imports go through the machine-wide key isolation service, so doing them per node start is the dominant cost of starting
/// a test cluster. The cached originals are owned here and released at process exit; callers receive copies they may dispose freely. Certificates that tests
/// need to differ from the shared ones (untrusted, expired, other authority) are not served from here.
/// </remarks>
[ThreadSafe]
internal static class SharedClusterCertificates
{
    private const int ValidityDays = 30;
    private const string AuthoritySubject = "CN=Squirix Cluster Test CA";

    private static readonly Lazy<X509Certificate2> Authority = new(CreateAuthority, LazyThreadSafetyMode.ExecutionAndPublication);
    private static readonly ConcurrentDictionary<string, Lazy<NodeMaterial>> Nodes = new(StringComparer.Ordinal);
    private static readonly ConcurrentDictionary<string, Lazy<NodeMaterial>> RsaNodes = new(StringComparer.Ordinal);

    static SharedClusterCertificates()
    {
        AppDomain.CurrentDomain.ProcessExit += static (_, _) => DisposeAll();
    }

    /// <summary>Gets the shared cluster CA, including its private key; callers must not dispose it.</summary>
    internal static X509Certificate2 ClusterAuthority => Authority.Value;

    /// <summary>Creates a copy of the shared CA public certificate for use as a trust anchor.</summary>
    /// <returns>A certificate without a private key that the caller owns.</returns>
    internal static X509Certificate2 CreateTrustAnchor() => X509CertificateLoader.LoadCertificate(Authority.Value.RawData);

    /// <summary>Creates a copy of the shared certificate of a node, including its private key.</summary>
    /// <param name="nodeId">Node identifier the certificate names.</param>
    /// <param name="rsaKey">Whether the node key is RSA instead of ECDSA P-256.</param>
    /// <returns>A certificate the caller owns.</returns>
    internal static X509Certificate2 CreateNodeCertificate(string nodeId, bool rsaKey = false) => new(GetNode(nodeId, rsaKey).Certificate);

    /// <summary>Gets the PFX bytes of the shared certificate of a node.</summary>
    /// <param name="nodeId">Node identifier the certificate names.</param>
    /// <param name="rsaKey">Whether the node key is RSA instead of ECDSA P-256.</param>
    /// <returns>PFX content without a password; the caller must not modify it.</returns>
    internal static byte[] GetNodePfx(string nodeId, bool rsaKey = false) => GetNode(nodeId, rsaKey).Pfx;

    private static NodeMaterial GetNode(string nodeId, bool rsaKey) =>
        (rsaKey ? RsaNodes : Nodes).GetOrAdd(nodeId, rsaKey ? static id => CreateLazyNode(id, true) : static id => CreateLazyNode(id, false)).Value;

    private static Lazy<NodeMaterial> CreateLazyNode(string nodeId, bool rsaKey) => new(() => NodeMaterial.Create(nodeId, rsaKey), LazyThreadSafetyMode.ExecutionAndPublication);

    private static X509Certificate2 CreateAuthority()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest(AuthoritySubject, key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        var notBefore = DateTimeOffset.UtcNow.AddDays(-1);
        return request.CreateSelfSigned(notBefore, notBefore.AddDays(ValidityDays));
    }

    private static void DisposeAll()
    {
        DisposeNodes(Nodes);
        DisposeNodes(RsaNodes);

        if (Authority.IsValueCreated)
            Authority.Value.Dispose();
    }

    private static void DisposeNodes(ConcurrentDictionary<string, Lazy<NodeMaterial>> nodes)
    {
        foreach (var node in nodes)
        {
            if (node.Value.IsValueCreated)
                node.Value.Value.Certificate.Dispose();
        }
    }

    [Immutable]
    private sealed class NodeMaterial
    {
        private NodeMaterial(X509Certificate2 certificate, byte[] pfx)
        {
            Certificate = certificate;
            Pfx = pfx;
        }

        internal X509Certificate2 Certificate { get; }

        internal byte[] Pfx { get; }

        internal static NodeMaterial Create(string nodeId, bool rsaKey)
        {
            using var fresh = TestCertificates.CreatePeerCertificate(Authority.Value, nodeId, rsaKey: rsaKey);
            var pfx = fresh.Export(X509ContentType.Pfx);
            return new NodeMaterial(X509CertificateLoader.LoadPkcs12(pfx, null, X509KeyStorageFlags.Exportable), pfx);
        }
    }
}
