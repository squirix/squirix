using System;
using System.Collections.Generic;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.JsonWebTokens;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class NativeConnectionAuthenticationValidator : IDisposable
{
    private readonly IOptionsMonitor<JwtValidationOptions> _options;
    private readonly TimeProvider _clock;
    private readonly IServiceScopeFactory _scopes;
    private readonly bool _hasAuthorizationHook;
    private readonly bool _hasTokenObserver;
    private readonly JsonWebTokenHandler _handler = new() { MapInboundClaims = true };
    private readonly ConnectionTrustHistory _history = new();
    private readonly SemaphoreSlim _pending = new(256, 256);

    internal NativeConnectionAuthenticationValidator(IOptionsMonitor<JwtValidationOptions> options, TimeProvider clock, IServiceProvider services)
    {
        _options = options;
        _clock = clock;
        _scopes = services.GetRequiredService<IServiceScopeFactory>();
        var registrations = services.GetRequiredService<IServiceProviderIsService>();
        _hasAuthorizationHook = registrations.IsService(typeof(IBinaryAuthorizationHook));
        _hasTokenObserver = registrations.IsService(typeof(IConnectionTokenObserver));
    }

    public void Dispose() => _pending.Dispose();

    internal AsyncServiceScope? CreateRequestScope() => _hasAuthorizationHook ? _scopes.CreateAsyncScope() : null;

    internal bool TryEnter() => _pending.Wait(0, CancellationToken.None);

    internal void Exit() => _pending.Release();

    internal async ValueTask<AuthenticatedConnection?> ValidateAsync(string token, BinaryConnection connection, CancellationToken cancellationToken)
    {
        var accepted = await ValidateTokenAsync(token, connection, cancellationToken).ConfigureAwait(false);
        return accepted == null
            ? null
            : AuthenticatedConnection.Create(
                accepted.Principal, (accepted.NotBefore, accepted.Expires), accepted.Options, accepted.Configuration, accepted.HistoricalStarted, connection.RequestClose);
    }

    internal async ValueTask<bool> ValidateBearerAsync(string token, CancellationToken cancellationToken)
    {
        if (!TryEnter())
            return false;
        try
        {
            var accepted = await ValidateTokenAsync(token, null, cancellationToken).ConfigureAwait(false);
            return accepted != null && HasAuthenticatedIdentity(accepted.Principal);
        }
        finally
        {
            Exit();
        }
    }

    internal async ValueTask<bool> IsCurrentAsync(AuthenticatedConnection connection, CancellationToken cancellationToken)
    {
        var options = _options.CurrentValue;
        if (!connection.IsWithinLifetime(_clock.GetUtcNow().UtcDateTime) || !ReferenceEquals(connection.Options, options))
            return false;

        var configuration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        var now = _clock.GetUtcNow().UtcDateTime;
        return connection.IsWithinLifetime(now) && ReferenceEquals(options, _options.CurrentValue) && ReferenceEquals(configuration, connection.Configuration) && ConnectionTrustHistory.IsCurrent(connection, now);
    }

    internal async ValueTask<FrameStatus> AuthorizeAsync(AuthenticatedConnection connection, BinaryRequestContext request, CancellationToken cancellationToken)
    {
        if (!await IsCurrentAsync(connection, cancellationToken).ConfigureAwait(false))
        {
            connection.RequestClose();
            return FrameStatus.Unavailable;
        }

        var principal = connection.ClonePrincipal();
        if (principal == null || !HasAuthenticatedIdentity(principal))
            return FrameStatus.Unauthenticated;

        request.User = principal;
        var hook = request.RequestServices?.GetService<IBinaryAuthorizationHook>();
        return hook == null ? FrameStatus.Ok : await hook.AuthorizeAsync(request).ConfigureAwait(false);
    }

    private static bool HasAuthenticatedIdentity(ClaimsPrincipal principal)
    {
        foreach (var identity in principal.Identities)
        {
            if (identity.IsAuthenticated)
                return true;
        }

        return false;
    }

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

    private static TokenValidationParameters CreateValidationParameters(JwtValidationOptions options, OpenIdConnectConfiguration? configuration)
    {
        var parameters = options.TokenValidationParameters.Clone();
        parameters.ConfigurationManager = options.ConfigurationManager as BaseConfigurationManager;
        if (parameters.ConfigurationManager == null && configuration != null)
        {
            parameters.ValidIssuers = new List<string>(parameters.ValidIssuers ?? []) { configuration.Issuer };
            var signingKeys = new List<SecurityKey>(parameters.IssuerSigningKeys ?? []);
            signingKeys.AddRange(configuration.SigningKeys);
            parameters.IssuerSigningKeys = signingKeys;
        }

        return parameters;
    }

    private static async ValueTask<OpenIdConnectConfiguration?> GetConfigurationAsync(JwtValidationOptions options, CancellationToken cancellationToken) =>
        options.ConfigurationManager == null ? null : await options.ConfigurationManager.GetConfigurationAsync(cancellationToken).ConfigureAwait(false);

    private async ValueTask<ValidatedToken?> ValidateTokenAsync(string raw, BinaryConnection? connection, CancellationToken cancellationToken)
    {
        var options = _options.CurrentValue;
        var configuration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        var started = _clock.GetUtcNow().UtcDateTime;
        var lifetime = (options.ConfigurationManager as BaseConfigurationManager)?.LastKnownGoodLifetime ?? TimeSpan.Zero;
        var parameters = CreateValidationParameters(options, configuration);

        var result = await _handler.ValidateTokenAsync(raw, parameters).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (!result.IsValid || result.ClaimsIdentity == null || result.SecurityToken == null)
            return null;

        var principal = new ClaimsPrincipal(result.ClaimsIdentity);
        if (_hasTokenObserver)
            principal = await ObservePrincipalAsync(principal, connection, cancellationToken).ConfigureAwait(false);
        if (principal == null || !ReferenceEquals(options, _options.CurrentValue))
            return null;

        var currentConfiguration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (!ReferenceEquals(options, _options.CurrentValue) || !ReferenceEquals(configuration, currentConfiguration))
            return null;

        var (notBefore, expires) = Lifetime(result.SecurityToken, parameters.ClockSkew);
        if (!_history.TryAuthorize(options, configuration, result.SecurityToken, started, lifetime, ref expires, out var historical))
            return null;

        var now = _clock.GetUtcNow().UtcDateTime;
        return now < notBefore || now >= expires ? null : new ValidatedToken(principal, notBefore, expires, options, configuration, historical);
    }

    private async ValueTask<ClaimsPrincipal?> ObservePrincipalAsync(ClaimsPrincipal principal, BinaryConnection? connection, CancellationToken cancellationToken)
    {
        var scope = _scopes.CreateAsyncScope();
        await using var scopeLifetime = scope.ConfigureAwait(false);
        var observer = scope.ServiceProvider.GetService<IConnectionTokenObserver>();
        return observer == null
            ? principal
            : await observer.OnValidatedAsync(principal, scope.ServiceProvider, connection, cancellationToken).ConfigureAwait(false);
    }

    private sealed record ValidatedToken(
        ClaimsPrincipal Principal,
        DateTime NotBefore,
        DateTime Expires,
        JwtValidationOptions Options,
        OpenIdConnectConfiguration? Configuration,
        DateTime? HistoricalStarted);
}
