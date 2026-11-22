using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>The shared test identity tracks the material for each startup's peer TLS options.</summary>
public sealed class ClusterIdentityTlsOptionsTests
{
    /// <summary>Peer options can be created while the identity lives. Disposing the identity ends their startup lifetime.</summary>
    /// <param name="profile">A profile that configures outbound peer TLS options.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(TestNodeProfile.NoOutboundClientCertificate)]
    [Arguments(TestNodeProfile.UntrustedOutboundClientCertificate)]
    [Arguments(TestNodeProfile.ExpiredPeerCertificate)]
    public async Task DisposeReleasesPeerOptions(TestNodeProfile profile, CancellationToken cancellationToken)
    {
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        var identity = new ClusterIdentity();
        try
        {
            var shared = identity;
            var peers = ClusterIdentity.CreatePeers(
                [new ClusterNode("nodeA", primaryA.HttpUri), new ClusterNode("nodeB", primaryB.HttpUri)],
                ref shared);
            var cluster = new TopologyOptions(peers) { NodeId = "nodeA", Uri = primaryA.HttpUri };
            var startup = await identity.ResolveNodeStartupForBindAsync(cluster, profile, cancellationToken);
            using var material = startup.Certificate;
            var factory = ThrowHelper.Required(startup.PeerOptionsFactory, "Expected outbound socket options.");
            _ = await Assert.That(factory("nodeB").Tls.RemoteCertificateValidationCallback).IsNotNull();

            identity.Dispose();

            _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(factory, static create => _ = create("nodeB"));
        }
        finally
        {
            identity.Dispose();
        }
    }

    /// <summary>Releasing a node ends its options lifetime and releases its material hold. The next startup can issue fresh options.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReleaseDisposesNodePeerOptions(CancellationToken cancellationToken)
    {
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        using var identity = new ClusterIdentity();
        var shared = identity;
        var peers = ClusterIdentity.CreatePeers(
            [new ClusterNode("nodeA", primaryA.HttpUri), new ClusterNode("nodeB", primaryB.HttpUri)],
            ref shared);
        var cluster = new TopologyOptions(peers) { NodeId = "nodeA", Uri = primaryA.HttpUri };
        var startup = await identity.ResolveNodeStartupForBindAsync(cluster, TestNodeProfile.NoOutboundClientCertificate, cancellationToken);
        var material = ThrowHelper.Required(startup.Certificate, "Expected loaded mTLS material.");
        var factory = ThrowHelper.Required(startup.PeerOptionsFactory, "Expected outbound socket options.");
        var firstOptions = factory("nodeB");
        _ = await Assert.That(firstOptions.Tls.RemoteCertificateValidationCallback).IsNotNull();

        // The loader release leaves the material available until the startup releases its hold.
        IDisposable loader = material;
        loader.Dispose();
        _ = await Assert.That(material.IsReleased).IsFalse();

        identity.PeerOptionLifetimes.Release(ThrowHelper.Required(startup.Owner, "Expected the startup to own its options."));

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(factory, static create => _ = create("nodeB"));
        _ = await Assert.That(material.IsReleased).IsTrue();

        var next = await identity.ResolveNodeStartupForBindAsync(cluster, TestNodeProfile.NoOutboundClientCertificate, cancellationToken);
        using var nextMaterial = next.Certificate;
        var nextFactory = ThrowHelper.Required(next.PeerOptionsFactory, "Expected fresh outbound socket options.");
        _ = await Assert.That(nextFactory("nodeB").Tls).IsNotSameReferenceAs(firstOptions.Tls);
    }

    /// <summary>A second startup or material lookup leaves the running startup's options and material usable until its own release.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task SecondStartupLeavesRunningPeerOptions(CancellationToken cancellationToken)
    {
        using var primaryA = ListenPortPool.ServerUnitTests.HoldPort();
        using var primaryB = ListenPortPool.ServerUnitTests.HoldPort();
        using var identity = new ClusterIdentity();
        var shared = identity;
        var peers = ClusterIdentity.CreatePeers(
            [new ClusterNode("nodeA", primaryA.HttpUri), new ClusterNode("nodeB", primaryB.HttpUri)],
            ref shared);
        var cluster = new TopologyOptions(peers) { NodeId = "nodeA", Uri = primaryA.HttpUri };
        var running = await identity.ResolveNodeStartupForBindAsync(cluster, TestNodeProfile.NoOutboundClientCertificate, cancellationToken);
        var factory = ThrowHelper.Required(running.PeerOptionsFactory, "Expected outbound socket options.");
        var firstOptions = factory("nodeB");
        _ = await Assert.That(firstOptions.Tls.RemoteCertificateValidationCallback).IsNotNull();

        var (_, _, lookupMaterial) = await ClusterIdentity.ResolveForNodeAsync(identity, cluster, cancellationToken);
        using var lookupHold = lookupMaterial;
        var second = await identity.ResolveNodeStartupForBindAsync(cluster, TestNodeProfile.NoOutboundClientCertificate, cancellationToken);
        using var secondMaterial = second.Certificate;

        _ = await Assert.That(factory("nodeB").Tls.RemoteCertificateValidationCallback).IsNotNull();

        identity.PeerOptionLifetimes.Release(ThrowHelper.Required(second.Owner, "Expected the second startup to own its options."));

        _ = await Assert.That(factory("nodeB").Tls.RemoteCertificateValidationCallback).IsNotNull();

        identity.PeerOptionLifetimes.Release(ThrowHelper.Required(running.Owner, "Expected the startup to own its options."));

        _ = NodeExceptionAssert.For<ObjectDisposedException>().Throws(factory, static create => _ = create("nodeB"));
    }
}
