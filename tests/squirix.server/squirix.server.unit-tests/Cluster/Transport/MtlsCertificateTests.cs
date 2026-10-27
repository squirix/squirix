using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Cluster.Transport;

/// <summary>Shared ownership of the cluster mTLS material: the certificates are freed by the last hold, whichever holder releases first.</summary>
[Immutable]
public sealed class MtlsCertificateTests : ServerUnitTestBase
{
    /// <summary>Disabled material has no certificates to hold.</summary>
    [Test]
    public async Task DisabledMaterialCannotBeRetained()
    {
        var material = MtlsCertificate.Load(new MtlsOptions(), null, false);

        _ = NodeExceptionAssert.For<InvalidOperationException>().Throws(material, static m => _ = m.Retain());
        _ = await Assert.That(material.IsReleased).IsTrue();
    }

    /// <summary>The loader's dispose releases its hold once; repeating it cannot steal the hold of another holder.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DisposeReleasesTheLoaderHoldOnce(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        using var hold = material.Retain();

        material.ReleaseHold();
        material.ReleaseHold();

        _ = await Assert.That(material.IsReleased).IsFalse();
        _ = await Assert.That(nodeCertificate.Handle).IsNotEqualTo(nint.Zero);
    }

    /// <summary>A hold releases once; repeating its dispose cannot free the certificates under the loader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task HoldReleasesOnce(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var hold = material.Retain();

        hold.Dispose();
        hold.Dispose();

        _ = await Assert.That(hold.IsReleased).IsTrue();
        _ = await Assert.That(material.IsReleased).IsFalse();
        material.ReleaseHold();
        _ = await Assert.That(material.IsReleased).IsTrue();
    }

    /// <summary>The certificates are freed when the loader releases after the last transport hold.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LastReleaseByLoaderFreesCertificates(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var hold = material.Retain();

        hold.Dispose();
        _ = await Assert.That(material.IsReleased).IsFalse();
        _ = await Assert.That(nodeCertificate.Handle).IsNotEqualTo(nint.Zero);

        material.ReleaseHold();
        _ = await Assert.That(material.IsReleased).IsTrue();
        _ = await Assert.That(nodeCertificate.Handle).IsEqualTo(nint.Zero);
    }

    /// <summary>The certificates are freed when the last transport hold releases after the loader.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task LastReleaseByHoldFreesCertificates(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        var first = material.Retain();
        var second = material.Retain();

        material.ReleaseHold();
        first.Dispose();
        _ = await Assert.That(material.IsReleased).IsFalse();
        _ = await Assert.That(nodeCertificate.Handle).IsNotEqualTo(nint.Zero);

        second.Dispose();
        _ = await Assert.That(material.IsReleased).IsTrue();
        _ = await Assert.That(nodeCertificate.Handle).IsEqualTo(nint.Zero);
    }

    /// <summary>Freed material cannot be retained again.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RetainAfterReleaseThrows(CancellationToken cancellationToken)
    {
        using var bundle = await MtlsTestCertificateFactory.CreateAsync(cancellationToken);
        using var nodeCertificate = MtlsTestCertificateFactory.CreatePeerCertificate(bundle.Ca, "node-a");
        using var material = MtlsCertificate.Create(nodeCertificate, bundle.Ca);
        material.ReleaseHold();

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(material, static m => _ = m.Retain());
    }
}
