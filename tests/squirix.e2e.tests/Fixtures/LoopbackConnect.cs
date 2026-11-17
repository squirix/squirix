using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Client;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Fixtures;

/// <summary>Connects the public SDK client to loopback HTTPS test nodes. Requires a trusted ASP.NET Core HTTPS development certificate on the host.</summary>
internal static class LoopbackConnect
{
    private static readonly SocketsHttpHandler SharedHandler = LoopbackHttp.CreateHandler();

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        var options = new SquirixClientOptions();
        options.Endpoints.Add(uri);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri primary, Uri secondary, CancellationToken cancellationToken)
    {
        var options = new SquirixClientOptions();
        options.Endpoints.Add(primary);
        options.Endpoints.Add(secondary);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }

    internal static async ValueTask<ISquirixClient> ConnectAsync(Uri uri, Func<CancellationToken, ValueTask<string>> bearerTokenProvider, CancellationToken cancellationToken)
    {
        var options = new SquirixClientOptions { BearerTokenProvider = bearerTokenProvider };
        options.Endpoints.Add(uri);
        var handler = LoopbackHttp.CreateHandler();
        try
        {
            var client = await SquirixClient.ConnectAsync(options, handler, cancellationToken);
            return new OwnedTransportClient(client, handler);
        }
        catch
        {
            handler.Dispose();
            throw;
        }
    }

    private sealed class OwnedTransportClient : ISquirixClient
    {
        private readonly ISquirixClient _client;
        private readonly SocketsHttpHandler _handler;

        internal OwnedTransportClient(ISquirixClient client, SocketsHttpHandler handler)
        {
            _client = client;
            _handler = handler;
        }

        public async ValueTask DisposeAsync()
        {
            try
            {
                await _client.DisposeAsync();
            }
            finally
            {
                _handler.Dispose();
            }
        }

        public ValueTask<ICache<T>> GetCacheAsync<T>(string cacheName, CancellationToken cancellationToken = default) => _client.GetCacheAsync<T>(cacheName, cancellationToken);
    }
}
