using System;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that a node whose loopback listen address cannot be bound fails startup.</summary>
public sealed class LoopbackBindFailureTests
{
    private const string NodeId = "nodeA";
    private const string ClientAuthenticationUsage = "1.3.6.1.5.5.7.3.2";
    private const int CertificateValidityDays = 1;

    /// <summary>A bind failure that is not an address-in-use error on an IP literal listen address fails node start instead of leaving the node serving elsewhere.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    /// <returns>A task that completes when the assertions finish.</returns>
    [Test]
    public async Task UnbindableLoopbackAddressFailsNodeStart(CancellationToken cancellationToken)
    {
        using var certificate = CreateClientOnlyCertificate();
        using var held = ListenPortPool.ServerUnitTests.HoldPort();
        ClusterIdentity? identity = null;
        var peers = ClusterIdentity.CreatePeers([new ClusterNode(NodeId, held.HttpUri)], ref identity);
        var cluster = new TopologyOptions(peers) { NodeId = NodeId, Uri = held.HttpUri };
        var options = new NodeHostStartOptions { ServerCertificate = certificate };
        held.Dispose();

        var start = NodeHost.StartAsync(cluster, options, cancellationToken);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(start);
    }

    private static X509Certificate2 CreateClientOnlyCertificate()
    {
        using var key = ECDsa.Create(ECCurve.NamedCurves.nistP256);
        var request = new CertificateRequest("CN=client-only", key, HashAlgorithmName.SHA256);
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension([new Oid(ClientAuthenticationUsage)], true));
        using var selfSigned = request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-CertificateValidityDays), DateTimeOffset.UtcNow.AddDays(CertificateValidityDays));
        return X509CertificateLoader.LoadPkcs12(selfSigned.Export(X509ContentType.Pfx), null);
    }
}
