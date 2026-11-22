using System;
using System.Collections.Generic;
using System.Threading;

namespace Squirix.Server.Adapters.Endpoint;

/// <summary>Keeps bounded, conservatively timed proof of configurations validated as current.</summary>
internal sealed class ConnectionTrustHistory
{
    private readonly List<Entry> _entries = [];
    private readonly Lock _gate = new();

    internal static bool IsCurrent(AuthenticatedConnection connection, DateTime now)
    {
        return connection.HistoricalStarted is not { } started ||
               (connection.Options.Metadata is { } manager &&
                manager.UseLastKnownGoodConfiguration &&
                now - started < manager.LastKnownGoodLifetime);
    }

    internal IReadOnlyList<JwtMetadata> GetCandidates(JwtValidationOptions options, DateTime now)
    {
        if (options.Metadata is not { UseLastKnownGoodConfiguration: true } manager)
            return [];
        lock (_gate)
        {
            var candidates = new List<JwtMetadata>();
            for (var index = 0; index < _entries.Count; index++)
            {
                var entry = _entries[index];
                if (ReferenceEquals(entry.Options, options) && now < entry.Expires && now < AddClamped(entry.Started, manager.LastKnownGoodLifetime))
                    candidates.Add(entry.Configuration);
            }

            return candidates;
        }
    }

    internal bool TryAuthorize(
        JwtValidationOptions options,
        JwtMetadata? configuration,
        TrustEvidence token,
        DateTime started,
        TimeSpan observedLifetime,
        ref DateTime expires,
        out DateTime? historicalStarted)
    {
        historicalStarted = null;
        if (configuration == null || options.Metadata is not { } manager)
            return true;
        if (!Matches(configuration, token, options))
        {
            return HasExplicitTrust(configuration, options, token) || (manager.UseLastKnownGoodConfiguration && TryHistorical(
                options,
                manager.LastKnownGoodLifetime,
                token,
                started,
                ref expires,
                out historicalStarted));
        }

        Record(configuration, options, started, observedLifetime < manager.LastKnownGoodLifetime ? observedLifetime : manager.LastKnownGoodLifetime);
        return true;
    }

    private static DateTime AddClamped(DateTime started, TimeSpan lifetime) =>
        lifetime.Ticks > DateTime.MaxValue.Ticks - started.Ticks ? DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc) : started.Add(lifetime);

    private static bool ContainsKey(JwtMetadata configuration, JwtVerificationKey? key)
    {
        for (var index = 0; index < configuration.SigningKeys.Count; index++)
        {
            var candidate = configuration.SigningKeys[index];
            if (ReferenceEquals(candidate, key))
                return true;
        }

        return false;
    }

    private static bool HasExplicitTrust(JwtMetadata configuration, JwtValidationOptions parameters, TrustEvidence token)
        => ReferenceEquals(parameters.SigningKey, token.SigningKey) && parameters.AcceptsIssuer(token.Issuer, configuration);

    private static bool Matches(JwtMetadata configuration, TrustEvidence token, JwtValidationOptions parameters) =>
        ContainsKey(configuration, token.SigningKey) && parameters.AcceptsIssuer(token.Issuer, configuration);

    private void Record(JwtMetadata configuration, JwtValidationOptions options, DateTime started, TimeSpan lifetime)
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

    private bool TryHistorical(JwtValidationOptions options, TimeSpan lifetime, TrustEvidence token, DateTime now, ref DateTime expires, out DateTime? started)
    {
        var tokenExpiry = expires;
        var selectedExpiry = DateTime.MinValue;
        DateTime? selectedStarted = null;
        lock (_gate)
        {
            for (var i = 0; i < _entries.Count; i++)
            {
                var entry = _entries[i];
                if (!ReferenceEquals(entry.Options, options) || !Matches(entry.Configuration, token, entry.Options))
                    continue;
                var candidate = AddClamped(entry.Started, lifetime);
                candidate = entry.Expires < candidate ? entry.Expires : candidate;
                candidate = tokenExpiry < candidate ? tokenExpiry : candidate;
                if (candidate <= now || candidate <= selectedExpiry)
                    continue;
                selectedExpiry = candidate;
                selectedStarted = entry.Started;
            }
        }

        started = selectedStarted;
        if (selectedStarted == null)
            return false;
        expires = selectedExpiry;
        return true;
    }

    private readonly record struct Entry(JwtMetadata Configuration, JwtValidationOptions Options, DateTime Started, DateTime Expires);
}
