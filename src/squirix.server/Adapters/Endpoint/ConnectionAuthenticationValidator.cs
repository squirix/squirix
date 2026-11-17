using System;
using System.IO;
using System.Net;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Connections;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Validates a credential once and monitors the trust snapshots used by its connection.</summary>
internal sealed class ConnectionAuthenticationValidator : IDisposable
{
    private readonly IOptionsMonitor<JwtBearerOptions> _options;
    private readonly TimeProvider _timeProvider;
    private readonly IServiceScopeFactory _scopeFactory;
    private readonly SemaphoreSlim _pending = new(256, 256);
    private readonly ConnectionTrustHistory _history = new();

    public ConnectionAuthenticationValidator(IOptionsMonitor<JwtBearerOptions> options, TimeProvider timeProvider, IServiceScopeFactory scopeFactory)
    {
        _options = options;
        _timeProvider = timeProvider;
        _scopeFactory = scopeFactory;
    }

    public void Dispose() => _pending.Dispose();

    internal bool TryEnter() => _pending.Wait(0, CancellationToken.None);

    internal void Exit() => _pending.Release();

    internal async ValueTask<AuthenticatedConnection?> ValidateAsync(string token, ConnectionContext connection, CancellationToken cancellationToken)
    {
        var options = _options.Get(JwtBearerDefaults.AuthenticationScheme);
        var configuration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        var started = TimeProvider.System.GetUtcNow().UtcDateTime;
        var lkgLifetime = options.ConfigurationManager is BaseConfigurationManager manager ? manager.LastKnownGoodLifetime : TimeSpan.Zero;
        var scope = _scopeFactory.CreateAsyncScope();
        await using var ownedScope = scope.ConfigureAwait(false);
        var handler = scope.ServiceProvider.GetRequiredService<ConnectionTokenHandler>();
        var context = CreateContext(connection, scope.ServiceProvider, token, cancellationToken);
        var accessor = scope.ServiceProvider.GetService<IHttpContextAccessor>();
        var previousContext = accessor?.HttpContext;
        accessor?.HttpContext = context;
        try
        {
            return await AuthenticateAsync(handler, context, options, (configuration, started, lkgLifetime), connection, cancellationToken).ConfigureAwait(false);
        }
        finally
        {
            accessor?.HttpContext = previousContext;
        }
    }

    internal async ValueTask<bool> IsCurrentAsync(AuthenticatedConnection connection, CancellationToken cancellationToken)
    {
        if (!connection.IsWithinLifetime(_timeProvider.GetUtcNow().UtcDateTime) ||
            !ReferenceEquals(connection.Options, _options.Get(JwtBearerDefaults.AuthenticationScheme)))
            return false;
        var current = await GetConfigurationAsync(connection.Options, cancellationToken).ConfigureAwait(false);
        return ReferenceEquals(current, connection.Configuration) && ConnectionTrustHistory.IsCurrent(connection, _timeProvider.GetUtcNow().UtcDateTime);
    }

    private static async Task<OpenIdConnectConfiguration?> GetConfigurationAsync(JwtBearerOptions options, CancellationToken cancellationToken) =>
        options.ConfigurationManager == null ? null : await options.ConfigurationManager.GetConfigurationAsync(cancellationToken).ConfigureAwait(false);

    private static DateTime AddClamped(DateTime value, TimeSpan offset)
    {
        return offset.Ticks switch
        {
            > 0 when DateTime.MaxValue.Ticks - value.Ticks < offset.Ticks => DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc),
            < 0 when value.Ticks < -offset.Ticks => DateTime.SpecifyKind(DateTime.MinValue, DateTimeKind.Utc),
            _ => value.Add(offset),
        };
    }

    private static (DateTime Lower, DateTime Upper) Lifetime(SecurityToken token, TimeSpan skew)
    {
        var lower = AddClamped(token.ValidFrom, -skew);
        var upper = AddClamped(token.ValidTo, skew);
        if (token.SigningKey is X509SecurityKey certificate)
        {
            var keyLower = AddClamped(certificate.Certificate.NotBefore.ToUniversalTime(), -skew);
            var keyUpper = AddClamped(certificate.Certificate.NotAfter.ToUniversalTime(), skew);
            lower = keyLower > lower ? keyLower : lower;
            upper = keyUpper < upper ? keyUpper : upper;
        }

        return (lower, upper);
    }

    private static DefaultHttpContext CreateContext(ConnectionContext connection, IServiceProvider services, string token, CancellationToken cancellationToken)
    {
        var features = new FeatureCollection(connection.Features);
        features.Set<IHttpRequestFeature>(new HttpRequestFeature());
        features.Set<IHttpResponseFeature>(new HttpResponseFeature());
        features.Set<IHttpResponseBodyFeature>(new StreamResponseBodyFeature(Stream.Null));
        features.Set<IHttpConnectionFeature>(new HttpConnectionFeature
        {
            ConnectionId = connection.ConnectionId,
            LocalIpAddress = (connection.LocalEndPoint as IPEndPoint)?.Address,
            RemoteIpAddress = (connection.RemoteEndPoint as IPEndPoint)?.Address,
            LocalPort = (connection.LocalEndPoint as IPEndPoint)?.Port ?? 0,
            RemotePort = (connection.RemoteEndPoint as IPEndPoint)?.Port ?? 0,
        });
        var context = new DefaultHttpContext(features) { RequestServices = services, RequestAborted = cancellationToken };
        context.Request.Scheme = "https";
        context.Request.Method = "AUTH";
        context.Request.Path = "/squirix/connection-authentication";
        context.Request.Headers.Authorization = $"Bearer {token}";
        return context;
    }

    private async ValueTask<AuthenticatedConnection?> AuthenticateAsync(ConnectionTokenHandler handler, HttpContext context, JwtBearerOptions options, (OpenIdConnectConfiguration? Configuration, DateTime Started, TimeSpan LkgLifetime) snapshot, ConnectionContext connection, CancellationToken cancellationToken)
    {
        var (configuration, started, lkgLifetime) = snapshot;
        await handler.InitializeAsync(new AuthenticationScheme(JwtBearerDefaults.AuthenticationScheme, null, typeof(ConnectionTokenHandler)), context).ConfigureAwait(false);
        var result = await handler.AuthenticateAsync().ConfigureAwait(false);
        if (!result.Succeeded || result.Principal == null || handler.ValidatedToken is not { } validated)
            return null;
        if (!ReferenceEquals(options, _options.Get(JwtBearerDefaults.AuthenticationScheme)) ||
            !ReferenceEquals(configuration, await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false)))
            return null;

        var (lower, upper) = Lifetime(validated, options.TokenValidationParameters.ClockSkew);
        if (!_history.TryAuthorize(options, configuration, validated, started, lkgLifetime, ref upper, out var historical))
            return null;
        var lifetime = connection.Features.Get<IConnectionLifetimeNotificationFeature>();
        if (lifetime == null)
            return null;
        var accepted = AuthenticatedConnection.Create(result.Principal, (lower, upper), options, configuration, historical, lifetime);
        return accepted?.IsWithinLifetime(_timeProvider.GetUtcNow().UtcDateTime) == true ? accepted : null;
    }
}
