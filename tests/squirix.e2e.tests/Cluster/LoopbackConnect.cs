using System;
using System.Net.Http;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Attributes;
using Squirix.Client;
using Squirix.Server.TestKit.Networking;

namespace Squirix.E2ETests.Cluster;

/// <summary>Connects the public SDK client to loopback HTTPS test nodes. Requires a trusted ASP.NET Core HTTPS development certificate on the host.</summary>
internal static class LoopbackConnect
{
    private static readonly SocketsHttpHandler SharedHandler = LoopbackHttp.CreateHandler();

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri uri, CancellationToken cancellationToken)
    {
        var configure = new EndpointConfigurator(uri);
        return ConnectAsync(configure.Apply, cancellationToken);
    }

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri primary, Uri secondary, CancellationToken cancellationToken)
    {
        var configure = new DualEndpointConfigurator(primary, secondary);
        return ConnectAsync(configure.Apply, cancellationToken);
    }

    internal static ValueTask<ISquirixClient> ConnectAsync(Uri uri, Func<CancellationToken, ValueTask<string>> bearerTokenProvider, CancellationToken cancellationToken)
    {
        var configure = new AuthenticatedEndpointConfigurator(uri, bearerTokenProvider);
        return ConnectAsync(configure.Apply, cancellationToken);
    }

    private static ValueTask<ISquirixClient> ConnectAsync(Action<SquirixClientOptions> configure, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(configure);
        var options = new SquirixClientOptions();
        configure(options);
        return SquirixClient.ConnectAsync(options, SharedHandler, cancellationToken);
    }

    [Immutable]
    private sealed class AuthenticatedEndpointConfigurator
    {
        private readonly Func<CancellationToken, ValueTask<string>> _bearerTokenProvider;
        private readonly Uri _uri;

        internal AuthenticatedEndpointConfigurator(Uri uri, Func<CancellationToken, ValueTask<string>> bearerTokenProvider)
        {
            _uri = uri;
            _bearerTokenProvider = bearerTokenProvider;
            Apply = ApplyCore;
        }

        internal Action<SquirixClientOptions> Apply { get; }

        private void ApplyCore(SquirixClientOptions options)
        {
            options.Endpoints.Add(_uri);
            options.BearerTokenProvider = _bearerTokenProvider;
        }
    }

    [Immutable]
    private sealed class DualEndpointConfigurator
    {
        private readonly Uri _primary;
        private readonly Uri _secondary;

        internal DualEndpointConfigurator(Uri primary, Uri secondary)
        {
            _primary = primary;
            _secondary = secondary;
            Apply = ApplyCore;
        }

        internal Action<SquirixClientOptions> Apply { get; }

        private void ApplyCore(SquirixClientOptions options)
        {
            options.Endpoints.Add(_primary);
            options.Endpoints.Add(_secondary);
        }
    }

    [Immutable]
    private sealed class EndpointConfigurator
    {
        private readonly Uri _uri;

        internal EndpointConfigurator(Uri uri)
        {
            _uri = uri;
            Apply = ApplyCore;
        }

        internal Action<SquirixClientOptions> Apply { get; }

        private void ApplyCore(SquirixClientOptions options) => options.Endpoints.Add(_uri);
    }
}
