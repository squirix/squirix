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

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri uri, Func<CancellationToken, ValueTask<string>> bearerTokenProvider, CancellationToken cancellationToken)
    {
        var options = new SquirixClientOptions { BearerTokenProvider = bearerTokenProvider };
        options.Endpoints.Add(uri);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }
}
