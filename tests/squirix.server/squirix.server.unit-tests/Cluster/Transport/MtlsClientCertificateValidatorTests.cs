using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for inbound cluster mTLS client certificate validation.</summary>
[Immutable]
public sealed class MtlsClientCertificateValidatorTests : ServerUnitTestBase
{
    /// <summary>Ensures inbound validation accepts configured remote peer identities only.</summary>
    [Fact]
    public async Task ValidatesPeerAgainstConfiguredNodeIds()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(peerCertificate, bundle.Ca, ["node-b", "node-c"]));
        Assert.False(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-c"));
    }

    /// <summary>Ensures expected node identity is enforced for peer certificates.</summary>
    [Fact]
    public async Task RejectsMismatchedPeerIdentity()
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(DefaultCancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        Assert.True(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-b"));
        Assert.False(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-c"));
    }
}
