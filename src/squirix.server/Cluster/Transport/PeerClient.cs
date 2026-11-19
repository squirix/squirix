using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Google.Protobuf;

namespace Squirix.Server.Cluster.Transport;

internal sealed class PeerClient : IPeerClient
{
    private readonly HttpClient _client;
    private readonly string? _fingerprint;

    internal PeerClient(HttpClient client, string? fingerprint)
    {
        _client = client;
        _fingerprint = fingerprint;
    }

    public Task<T> SendAsync<T>(string path, IMessage request, MessageParser<T> parser, CancellationToken cancellationToken)
        where T : IMessage<T> => PeerProtocol.SendAsync(_client, path, request, parser, _fingerprint, cancellationToken);
}
