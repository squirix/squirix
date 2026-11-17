using System;
using System.Collections.Generic;
using System.Threading;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Keeps bounded, conservatively timed proof of configurations validated as current.</summary>
internal sealed class ConnectionTrustHistory
{
    private readonly Lock _gate = new();
    private readonly List<Entry> _entries = [];

    internal static bool IsCurrent(AuthenticatedConnection connection, DateTime now)
    {
        return connection.HistoricalStarted is not { } started ||
               (connection.Options.ConfigurationManager is BaseConfigurationManager manager &&
                manager.UseLastKnownGoodConfiguration &&
                now - started < manager.LastKnownGoodLifetime);
    }

    internal bool TryAuthorize(
        JwtBearerOptions options,
        OpenIdConnectConfiguration? configuration,
        SecurityToken token,
        DateTime started,
        TimeSpan observedLifetime,
        ref DateTime expires,
        out DateTime? historicalStarted)
    {
        historicalStarted = null;
        if (configuration == null || options.ConfigurationManager is not BaseConfigurationManager manager)
            return true;
        if (Matches(configuration, token))
        {
            Record(configuration, options, started, observedLifetime < manager.LastKnownGoodLifetime ? observedLifetime : manager.LastKnownGoodLifetime);
            return true;
        }

        return HasExplicitTrust(options.TokenValidationParameters, token) ||
               (manager.UseLastKnownGoodConfiguration &&
                TryHistorical(options, manager.LastKnownGoodLifetime, token, started, ref expires, out historicalStarted));
    }

    private static bool Matches(BaseConfiguration configuration, SecurityToken token) =>
        string.Equals(configuration.Issuer, token.Issuer, StringComparison.Ordinal) && ContainsKey(configuration, token.SigningKey);

    private static bool ContainsKey(BaseConfiguration configuration, SecurityKey? key)
    {
        foreach (var candidate in configuration.SigningKeys)
        {
            if (ReferenceEquals(candidate, key))
                return true;
        }

        return false;
    }

    private static bool HasExplicitTrust(TokenValidationParameters parameters, SecurityToken token)
    {
        if (!ContainsExplicitKey(parameters, token.SigningKey))
            return false;
        if (string.Equals(parameters.ValidIssuer, token.Issuer, StringComparison.Ordinal))
            return true;
        if (parameters.ValidIssuers != null)
        {
            foreach (var issuer in parameters.ValidIssuers)
            {
                if (string.Equals(issuer, token.Issuer, StringComparison.Ordinal))
                    return true;
            }
        }

        return false;
    }

    private static bool ContainsExplicitKey(TokenValidationParameters parameters, SecurityKey? key)
    {
        if (ReferenceEquals(parameters.IssuerSigningKey, key))
            return true;
        if (parameters.IssuerSigningKeys != null)
        {
            foreach (var candidate in parameters.IssuerSigningKeys)
            {
                if (ReferenceEquals(candidate, key))
                    return true;
            }
        }

        return false;
    }

    private static DateTime AddClamped(DateTime started, TimeSpan lifetime) =>
        lifetime.Ticks > DateTime.MaxValue.Ticks - started.Ticks ? DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc) : started.Add(lifetime);

    private void Record(BaseConfiguration configuration, JwtBearerOptions options, DateTime started, TimeSpan lifetime)
    {
        lock (_gate)
        {
            for (var i = 0; i < _entries.Count; i++)
            {
                if (!ReferenceEquals(_entries[i].Configuration, configuration) || !ReferenceEquals(_entries[i].Options, options))
                    continue;
                if (started > _entries[i].Started)
                    _entries[i] = new Entry(configuration, options, started, AddClamped(started, lifetime));
                return;
            }

            if (_entries.Count == 16)
                _entries.RemoveAt(0);
            _entries.Add(new Entry(configuration, options, started, AddClamped(started, lifetime)));
        }
    }

    private bool TryHistorical(JwtBearerOptions options, TimeSpan lifetime, SecurityToken token, DateTime now, ref DateTime expires, out DateTime? started)
    {
        lock (_gate)
        {
            for (var i = 0; i < _entries.Count; i++)
            {
                var entry = _entries[i];
                if (!ReferenceEquals(entry.Options, options) || !Matches(entry.Configuration, token))
                    continue;
                var until = AddClamped(entry.Started, lifetime);
                until = entry.Expires < until ? entry.Expires : until;
                expires = until < expires ? until : expires;
                started = entry.Started;
                return expires > now;
            }
        }

        started = null;
        return false;
    }

    private readonly record struct Entry(BaseConfiguration Configuration, JwtBearerOptions Options, DateTime Started, DateTime Expires);
}
