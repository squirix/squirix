using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Node.Backpressure;
using Squirix.Transport.Binary;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed class NativeConnectionAuthenticationValidator : IDisposable
{
    private readonly TimeProvider _clock;
    private readonly bool _hasAuthorizationHook;
    private readonly bool _hasTokenObserver;
    private readonly ConnectionTrustHistory _history = new();
    private readonly Func<JwtValidationOptions> _options;
    private readonly bool _requiresRequestPrincipal;
    private readonly SemaphoreSlim _pending = new(256, 256);
    private readonly INodeRequestScopes? _scopes;

    internal NativeConnectionAuthenticationValidator(Func<JwtValidationOptions> options, TimeProvider clock, IServiceProvider services)
    {
        _options = options;
        _clock = clock;
        _scopes = services.Find<INodeRequestScopes>();
        _hasAuthorizationHook = _scopes?.Contains(typeof(IBinaryAuthorizationHook)) == true;
        _hasTokenObserver = _scopes?.Contains(typeof(IConnectionTokenObserver)) == true;
        _requiresRequestPrincipal = _hasAuthorizationHook || _hasTokenObserver ||
                                    services.Find<IBackpressureClientIdResolver>() is not SharedClientIdResolver ||
                                    services.Find<BinaryCallObserver>() != null;
    }

    public void Dispose() => _pending.Dispose();

    internal async ValueTask<FrameStatus> AuthorizeAsync(AuthenticatedConnection connection, BinaryRequestContext request, CancellationToken cancellationToken)
    {
        if (!await IsCurrentAsync(connection, cancellationToken).ConfigureAwait(false))
        {
            connection.RequestClose();
            return FrameStatus.Unavailable;
        }

        // ValidateTokenAsync already verified the identity accepted for this connection.
        if (!_requiresRequestPrincipal)
            return FrameStatus.Ok;

        var principal = connection.ClonePrincipal();
        if (principal == null || !HasAuthenticatedIdentity(principal))
            return FrameStatus.Unauthenticated;

        request.User = principal;
        var hook = request.RequestServices?.Find<IBinaryAuthorizationHook>();
        return hook == null ? FrameStatus.Ok : await hook.AuthorizeAsync(request).ConfigureAwait(false);
    }

    internal NodeRequestScope? CreateRequestScope() => _hasAuthorizationHook ? _scopes!.Open() : null;

    internal void Exit() => _pending.Release();

    internal async ValueTask<bool> IsCurrentAsync(AuthenticatedConnection connection, CancellationToken cancellationToken)
    {
        var options = _options();
        if (!connection.IsWithinLifetime(_clock.GetUtcNow().UtcDateTime) || !ReferenceEquals(connection.Options, options))
            return false;

        var configuration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        var now = _clock.GetUtcNow().UtcDateTime;
        return connection.IsWithinLifetime(now) && ReferenceEquals(options, _options()) && ReferenceEquals(configuration, connection.Configuration) &&
               ConnectionTrustHistory.IsCurrent(connection, now);
    }

    internal bool TryEnter() => _pending.Wait(0, CancellationToken.None);

    internal async ValueTask<AuthenticatedConnection?> ValidateAsync(string token, BinaryConnection connection, CancellationToken cancellationToken)
    {
        var accepted = await ValidateTokenAsync(token, connection, cancellationToken).ConfigureAwait(false);
        return accepted == null
            ? null
            : AuthenticatedConnection.Create(
                accepted.Principal,
                (accepted.NotBefore, accepted.Expires),
                accepted.Options,
                accepted.Configuration,
                accepted.HistoricalStarted,
                connection.RequestClose);
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

    private static async ValueTask<JwtMetadata?> GetConfigurationAsync(JwtValidationOptions options, CancellationToken cancellationToken) =>
        options.Metadata == null ? null : await options.Metadata.GetConfigurationAsync(cancellationToken).ConfigureAwait(false);

    private static bool HasAuthenticatedIdentity(ClaimsPrincipal principal)
    {
        foreach (var identity in principal.Identities)
        {
            if (identity.IsAuthenticated)
                return true;
        }

        return false;
    }

    private async ValueTask<ClaimsPrincipal?> ObservePrincipalAsync(ClaimsPrincipal principal, BinaryConnection? connection, CancellationToken cancellationToken)
    {
        var scope = _scopes!.Open();
        await using var scopeLifetime = scope.ConfigureAwait(false);
        var observer = scope.ServiceProvider.Find<IConnectionTokenObserver>();
        return observer == null ? principal : await observer.OnValidatedAsync(principal, scope.ServiceProvider, connection, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<ValidatedToken?> ValidateTokenAsync(string raw, BinaryConnection? connection, CancellationToken cancellationToken)
    {
        var options = _options();
        var configuration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        var started = _clock.GetUtcNow().UtcDateTime;
        var lifetime = options.Metadata?.LastKnownGoodLifetime ?? TimeSpan.Zero;
        var result = VerifyToken(raw, options, configuration, started, cancellationToken);
        cancellationToken.ThrowIfCancellationRequested();
        if (result == null)
            return null;

        var principal = result.Principal;
        if (_hasTokenObserver)
            principal = await ObservePrincipalAsync(principal, connection, cancellationToken).ConfigureAwait(false);
        if (principal == null || !HasAuthenticatedIdentity(principal) || !ReferenceEquals(options, _options()))
            return null;

        var currentConfiguration = await GetConfigurationAsync(options, cancellationToken).ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        if (!ReferenceEquals(options, _options()) || !ReferenceEquals(configuration, currentConfiguration))
            return null;

        var notBefore = result.NotBefore;
        var expires = result.Expires;
        if (!_history.TryAuthorize(options, configuration, new TrustEvidence(result.Issuer, result.SigningKey), started, lifetime, ref expires, out var historical))
            return null;

        var now = _clock.GetUtcNow().UtcDateTime;
        return now < notBefore || now >= expires ? null : new ValidatedToken(principal, notBefore, expires, options, configuration, historical);
    }

    private SignedConnectionToken? VerifyToken(string raw, JwtValidationOptions options, JwtMetadata? configuration, DateTime started, CancellationToken cancellationToken)
    {
        var result = SignedConnectionToken.Verify(raw, options, configuration, started, out var refresh);
        if (result != null || !refresh || options.Metadata == null)
            return result;
        options.Metadata.RequestRefresh();
        foreach (var candidate in _history.GetCandidates(options, started))
        {
            cancellationToken.ThrowIfCancellationRequested();
            result = SignedConnectionToken.Verify(raw, options, candidate, started, out _);
            if (result != null && ReferenceEquals(result.SigningKey, options.SigningKey))
                result = null;
            if (result != null)
                break;
        }

        return result;
    }

    private sealed record ValidatedToken(
        ClaimsPrincipal Principal,
        DateTime NotBefore,
        DateTime Expires,
        JwtValidationOptions Options,
        JwtMetadata? Configuration,
        DateTime? HistoricalStarted);
}
