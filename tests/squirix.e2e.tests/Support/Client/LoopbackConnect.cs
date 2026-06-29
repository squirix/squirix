using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Support.Client;

/// <summary>Connects the public SDK client to loopback HTTPS test nodes. Requires a trusted ASP.NET Core HTTPS development certificate on the host.</summary>
internal static class LoopbackConnect
{
    private static readonly SocketsHttpHandler SharedHandler = LoopbackHttp.CreateHandler();

    public static ValueTask<ISquirixClient> ConnectAsync(Uri uri, CancellationToken cancellationToken) => ConnectAsync(options => options.Endpoints.Add(uri), cancellationToken);

    public static ValueTask<ISquirixClient> ConnectAsync(Action<SquirixOptions> configure, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new SquirixOptions();
        configure(options);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }
}
