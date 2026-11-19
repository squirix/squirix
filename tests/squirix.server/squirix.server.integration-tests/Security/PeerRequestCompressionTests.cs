using System.IO;
using System.IO.Compression;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.Core;
using Squirix.Server.IntegrationTests.Support;
using Squirix.Server.Serialization.Protobuf;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Mtls;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Security;

/// <summary>Verifies the peer listener rejects compressed requests.</summary>
public sealed class PeerRequestCompressionTests : NodeIntegrationTestBase
{
    /// <summary>Compressed peer requests are refused before cache dispatch.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CompressedRequestIsRefused(CancellationToken cancellationToken)
    {
        await using var cluster = await StartClusterAsync("A", "B", cancellationToken: cancellationToken);
        var peer = cluster.Peers[0];
        var identity = cluster["B"].GetRequiredService<MtlsCertificate>();
        using var handler = TestCertificates.CreateMtlsHandler(identity.NodeCertificate!, identity.TrustAnchor!, peer.NodeId);
        using var client = new HttpClient(handler, false) { BaseAddress = peer.InterNodeUri };
        var payload = new SetEntryAsyncRequest
        {
            OperationId = RpcOperationIdentity.New(),
            CacheName = "default",
            Key = "compressed-request",
            Entry = new NodeCacheEntry<object?> { Value = "compressed", Version = 1 }.MapToProto(),
        };
        await using var bytes = new MemoryStream();
        await using (var gzip = new GZipStream(bytes, CompressionLevel.Fastest, true))
            payload.WriteTo(gzip);
        using var request = new HttpRequestMessage(HttpMethod.Post, PeerProtocol.Prefix + "owner/SetEntry")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent(bytes.ToArray()),
        };
        request.Content.Headers.ContentType = new MediaTypeHeaderValue(PeerProtocol.MediaType);
        request.Content.Headers.ContentEncoding.Add("gzip");
        using var response = await client.SendAsync(request, cancellationToken);

        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.UnsupportedMediaType);
        using var channel = CreateBinaryChannel(cluster["A"].Uri);
        var plain = new CacheWireClient(channel);
        var result = await plain.GetEntryAsync(new GetEntryAsyncRequest { CacheName = "default", Key = payload.Key }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
    }
}
