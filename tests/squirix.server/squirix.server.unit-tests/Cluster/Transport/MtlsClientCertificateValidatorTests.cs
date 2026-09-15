using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Unit tests for inbound cluster mTLS client certificate validation.</summary>
[Immutable]
public sealed class MtlsClientCertificateValidatorTests : ServerUnitTestBase
{
    /// <summary>Ensures expected node identity is enforced for peer certificates.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsMismatchedPeerIdentity(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        _ = await Assert.That(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-b")).IsTrue();
        _ = await Assert.That(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-c")).IsFalse();
    }

    /// <summary>Ensures inbound validation accepts configured remote peer identities only.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ValidatesPeerAgainstConfiguredNodeIds(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var peerCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-b");

        _ = await Assert.That(MtlsClientCertificateValidator.ValidateForConfiguredRemotePeer(peerCertificate, bundle.Ca, ["node-b", "node-c"])).IsTrue();
        _ = await Assert.That(MtlsClientCertificateValidator.ValidateForExpectedNodeId(peerCertificate, bundle.Ca, "node-c")).IsFalse();
    }
}
