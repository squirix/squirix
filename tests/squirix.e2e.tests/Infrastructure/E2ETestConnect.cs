using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.TestKit.Http;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Connects E2E tests to loopback HTTPS nodes. Requires a trusted ASP.NET Core HTTPS development certificate on the host.
/// </summary>
internal static class E2ETestConnect
{
    private static readonly SocketsHttpHandler SharedHandler = LoopbackHttp.CreateHandler();

    public static ValueTask<ISquirixClient> ConnectAsync(string endpoint, CancellationToken cancellationToken)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(endpoint);
        return ConnectAsync(options => options.Endpoints.Add(endpoint), cancellationToken);
    }

    public static ValueTask<ISquirixClient> ConnectAsync(Action<SquirixOptions> configure, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new SquirixOptions();
        configure(options);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }
}
