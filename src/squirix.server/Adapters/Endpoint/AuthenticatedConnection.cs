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

    internal AuthenticatedConnection(
        ClaimsPrincipal principal,
        (DateTime NotBefore, DateTime Expires) bounds,
        JwtBearerOptions options,
        OpenIdConnectConfiguration? configuration,
        DateTime? historicalStarted,
        IConnectionLifetimeNotificationFeature lifetime)
    {
        Principal = principal;
        NotBefore = bounds.NotBefore;
        Expires = bounds.Expires;
        Options = options;
        Configuration = configuration;
        HistoricalStarted = historicalStarted;
        _lifetime = lifetime;
    }

    internal ClaimsPrincipal Principal { get; }

    internal DateTime NotBefore { get; }

    internal DateTime Expires { get; }

    internal JwtBearerOptions Options { get; }

    internal OpenIdConnectConfiguration? Configuration { get; }

    internal DateTime? HistoricalStarted { get; }

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
}
