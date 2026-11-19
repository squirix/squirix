using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
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
        using var handler = TestCertificates.CreateMtlsHandler(identity.NodeCertificate!, identity.TrustAnchor!, peer.NodeId);
        using var client = new HttpClient(handler, false) { BaseAddress = peer.InterNodeUri };

        var result = await PeerProtocol.SendAsync(client, "ping", new Empty(), Empty.Parser, null, cancellationToken);

        _ = await Assert.That(result).IsEqualTo(new Empty());
    }

    /// <summary>The internal listener refuses an unknown version and an invalid timeout before dispatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task RejectsUnknownVersionAndInvalidBudget(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("A", "B", static _ => new BlackBoxStartOptions(), cancellationToken);
        var identity = cluster["B"].GetRequiredService<MtlsCertificate>();
        var peer = cluster.Peers[0];
        using var handler = TestCertificates.CreateMtlsHandler(identity.NodeCertificate!, identity.TrustAnchor!, peer.NodeId);
        using var client = new HttpClient(handler, false) { BaseAddress = peer.InterNodeUri };
        using var unknown = CreateRequest("/squirix/peer/v2/ping");
        using var invalid = CreateRequest(PeerProtocol.Prefix + "ping");
        invalid.Headers.Add(PeerProtocol.BudgetHeader, "-1");

        using var unknownResponse = await client.SendAsync(unknown, cancellationToken);
        using var invalidResponse = await client.SendAsync(invalid, cancellationToken);

        _ = await Assert.That(unknownResponse.StatusCode).IsEqualTo(HttpStatusCode.NotFound);
        _ = await Assert.That(invalidResponse.StatusCode).IsEqualTo(HttpStatusCode.BadRequest);
    }

    private static HttpRequestMessage CreateRequest(string path)
    {
        var request = new HttpRequestMessage(HttpMethod.Post, path)
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent([]),
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(PeerProtocol.MediaType);
        return request;
    }
}
