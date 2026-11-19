using System;
using System.Net;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Cluster.Transport;
using Squirix.Server.TestKit.Networking;

namespace Squirix.Server.TestKit.Mtls;

internal static class InterNodeProbe
{
    internal static async Task<HttpStatusCode?> GetValueAsync(Uri uri, CancellationToken cancellationToken)
    {
        using var handler = LoopbackHttp.CreateHandler();
        using var client = new HttpClient(handler, false) { BaseAddress = uri };
        using var request = new HttpRequestMessage(HttpMethod.Post, PeerProtocol.Prefix + "owner/GetValue")
        {
            Version = HttpVersion.Version20,
            VersionPolicy = HttpVersionPolicy.RequestVersionExact,
            Content = new ByteArrayContent([]),
        };
        request.Headers.Add("squirix-internal-owner-rpc", "true");
        request.Content.Headers.ContentType = new System.Net.Http.Headers.MediaTypeHeaderValue(PeerProtocol.MediaType);
        using var response = await client.SendAsync(request, cancellationToken).ConfigureAwait(false);
        return response.StatusCode;
    }
}
