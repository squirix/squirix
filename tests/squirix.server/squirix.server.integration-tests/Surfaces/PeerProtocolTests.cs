using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Exercises the private peer protocol over a real mTLS connection.</summary>
public sealed class PeerProtocolTests : ServerSurfaceTestBase
{
    /// <summary>A verified node reads a versioned response from the internal listener.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task VerifiedPeerReceivesVersionedResponse(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("A", "B", static _ => new BlackBoxStartOptions(), cancellationToken);
        var identity = cluster["B"].GetRequiredService<MtlsCertificate>();
        var peer = cluster.Peers[0];
        var tlsOptions = TestCertificates.CreatePeerTlsOptions([identity.NodeCertificate!], identity.TrustAnchor!, peer.NodeId);
        var client = new PeerSocketProbe(peer.InterNodeUri!, tlsOptions);

        var result = await client.SendAsync(PeerOperation.Ping, new SetAsyncResponse(), SetAsyncResponse.Parser, null, cancellationToken);

        _ = await Assert.That(result.CalculateSize()).IsEqualTo(0);
    }

    /// <summary>The internal listener refuses an unknown version and an invalid timeout before dispatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsUnknownVersionAndInvalidBudget(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("A", "B", static _ => new BlackBoxStartOptions(), cancellationToken);
        var identity = cluster["B"].GetRequiredService<MtlsCertificate>();
        var peer = cluster.Peers[0];
        var tlsOptions = TestCertificates.CreatePeerTlsOptions([identity.NodeCertificate!], identity.TrustAnchor!, peer.NodeId);
        var client = new PeerSocketProbe(peer.InterNodeUri!, tlsOptions);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(client.SendRawAsync(PeerOperation.Ping, [], null, -1, true, cancellationToken), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(client.SendRawAsync(PeerOperation.Ping, [], null, -2, false, cancellationToken), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await NodeAsyncAssert.ThrowsAnyAsync<IOException>(client.SendRawAsync(PeerOperation.Ping, [], null, long.MaxValue, false, cancellationToken), TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
    }
}
