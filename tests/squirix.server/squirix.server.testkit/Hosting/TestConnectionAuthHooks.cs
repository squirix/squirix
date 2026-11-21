using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Transport.Binary;

namespace Squirix.Server.TestKit.Hosting;

/// <summary>Registers native authentication probes for tests that use the public host fixture.</summary>
public static class TestConnectionAuthHooks
{
    /// <summary>Observes a successful handshake and optionally gates the following cache operations.</summary>
    /// <param name="services">The node service registrations.</param>
    /// <param name="onConnection">Receives the graceful connection close callback.</param>
    /// <param name="onAuthorize">Optional callback awaited during authorization of each cache operation.</param>
    internal static void Add(
        TestServiceCollection services,
        Action<Action> onConnection,
        Func<CancellationToken, Task>? onAuthorize = null)
    {
        _ = services.AddSingleton<IConnectionTokenObserver>(new HandshakeObserver(onConnection));
        if (onAuthorize != null)
            _ = services.AddSingleton<IBinaryAuthorizationHook>(new AuthorizationObserver(onAuthorize));
    }

    private sealed class HandshakeObserver : IConnectionTokenObserver
    {
        private readonly Action<Action> _onConnection;

        internal HandshakeObserver(Action<Action> onConnection)
        {
            _onConnection = onConnection;
        }

        public ValueTask<ClaimsPrincipal?> OnValidatedAsync(
            ClaimsPrincipal principal,
            IServiceProvider services,
            BinaryConnection? connection,
            CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (connection != null)
                _onConnection(connection.RequestClose);
            return ValueTask.FromResult<ClaimsPrincipal?>(principal);
        }
    }

    private sealed class AuthorizationObserver : IBinaryAuthorizationHook
    {
        private readonly Func<CancellationToken, Task> _onAuthorize;

        internal AuthorizationObserver(Func<CancellationToken, Task> onAuthorize)
        {
            _onAuthorize = onAuthorize;
        }

        public async ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request)
        {
            await _onAuthorize(request.RequestAborted).ConfigureAwait(false);
            return FrameStatus.Ok;
        }
    }
}
