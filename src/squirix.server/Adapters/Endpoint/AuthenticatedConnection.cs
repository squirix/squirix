using System;
using System.Security.Claims;
using System.Threading;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Owns the immutable identity and validation snapshot accepted for one TLS transport.</summary>
internal sealed class AuthenticatedConnection
{
    private readonly IConnectionLifetimeNotificationFeature _lifetime;
    private readonly ClaimsPrincipal _principal;

    private AuthenticatedConnection(
        ClaimsPrincipal principal,
        (DateTime NotBefore, DateTime Expires) bounds,
        JwtBearerOptions options,
        OpenIdConnectConfiguration? configuration,
        DateTime? historicalStarted,
        IConnectionLifetimeNotificationFeature lifetime)
    {
        _principal = principal;
        NotBefore = bounds.NotBefore;
        Expires = bounds.Expires;
        Options = options;
        Configuration = configuration;
        HistoricalStarted = historicalStarted;
        _lifetime = lifetime;
    }

    internal DateTime NotBefore { get; }

    internal DateTime Expires { get; }

    internal JwtBearerOptions Options { get; }

    internal OpenIdConnectConfiguration? Configuration { get; }

    internal DateTime? HistoricalStarted { get; }

    internal static AuthenticatedConnection? Create(
        ClaimsPrincipal principal,
        (DateTime NotBefore, DateTime Expires) bounds,
        JwtBearerOptions options,
        OpenIdConnectConfiguration? configuration,
        DateTime? historicalStarted,
        IConnectionLifetimeNotificationFeature lifetime)
    {
        var snapshot = CopyPrincipal(principal);
        return snapshot == null ? null : new AuthenticatedConnection(snapshot, bounds, options, configuration, historicalStarted, lifetime);
    }

    internal ClaimsPrincipal? ClonePrincipal() => CopyPrincipal(_principal);

    internal bool IsWithinLifetime(DateTime now) => now >= NotBefore && now < Expires;

    internal void RequestClose() => _lifetime.RequestClose();

    internal ITimer CreateExpiryTimer(TimeProvider timeProvider)
    {
        var due = Expires - timeProvider.GetUtcNow().UtcDateTime;
        if (due < TimeSpan.Zero)
            due = TimeSpan.Zero;
        var maximum = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
        if (due > maximum)
            due = maximum;
        return timeProvider.CreateTimer(static state => { if (state is AuthenticatedConnection connection) connection.RequestClose(); }, this, due, Timeout.InfiniteTimeSpan);
    }

    private static ClaimsPrincipal? CopyPrincipal(ClaimsPrincipal principal)
    {
        if (principal.GetType() != typeof(ClaimsPrincipal))
        {
            var custom = principal.Clone();
            var supported = custom != null && !ReferenceEquals(custom, principal) && custom.GetType() == principal.GetType() &&
                            HasIndependentIdentities(principal, custom);
            return supported ? custom : null;
        }

        var clone = new ClaimsPrincipal();
        foreach (var identity in principal.Identities)
        {
            var copy = identity.Clone();
            if (copy == null || copy.GetType() != identity.GetType() || SharesIdentity(principal, copy))
                return null;
            clone.AddIdentity(copy);
        }

        return clone;
    }

    private static bool HasIndependentIdentities(ClaimsPrincipal principal, ClaimsPrincipal clone)
    {
        using var copies = clone.Identities.GetEnumerator();
        foreach (var identity in principal.Identities)
        {
            if (!copies.MoveNext() || copies.Current == null || copies.Current.GetType() != identity.GetType() || SharesIdentity(principal, copies.Current))
                return false;
        }

        return !copies.MoveNext();
    }

    private static bool SharesIdentity(ClaimsPrincipal principal, ClaimsIdentity candidate)
    {
        foreach (var identity in principal.Identities)
        {
            if (ReferenceEquals(identity, candidate))
                return true;
        }

        return false;
    }
}
