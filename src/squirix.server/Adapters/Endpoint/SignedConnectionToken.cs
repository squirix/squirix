using System;
using System.Buffers;
using System.Buffers.Text;
using System.Collections.Generic;
using System.Globalization;
using System.Security.Claims;
using System.Text;
using System.Text.Json;

namespace Squirix.Server.Adapters.Endpoint;

internal sealed record SignedConnectionToken(string Issuer, JwtVerificationKey SigningKey, ClaimsPrincipal Principal, DateTime NotBefore, DateTime Expires)
{
    private static readonly TimeSpan ClockSkew = TimeSpan.FromMinutes(2);

    private static readonly SearchValues<char> CompactCharacters = SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_.");

    internal static SignedConnectionToken? Verify(string raw, JwtValidationOptions options, JwtMetadata? metadata, DateTime now, out bool refresh)
    {
        refresh = false;
        if (raw.Length > 256 * 1024 || raw.AsSpan().ContainsAnyExcept(CompactCharacters))
            return null;
        var first = raw.IndexOf('.', StringComparison.Ordinal);
        var last = raw.AsSpan().LastIndexOf('.');
        if (first <= 0 || last <= first + 1 || last >= raw.Length - 1 || raw.AsSpan(first + 1, last - first - 1).Contains('.'))
            return null;
        try
        {
            var verifiedKey = VerifySignature(raw, first, last, options, metadata, out refresh);
            if (verifiedKey == null)
                return null;

            var payload = Base64Url.DecodeFromChars(raw.AsSpan(first + 1, last - first - 1));
            return ValidateClaims(payload, verifiedKey, options, metadata, now, out refresh);
        }
        catch (Exception ex) when (ex is FormatException or JsonException or KeyNotFoundException or ArgumentException or InvalidOperationException or OverflowException)
        {
            return null;
        }
    }

    internal static DateTime Offset(DateTime value, TimeSpan offset) =>
        new(Math.Clamp(value.Ticks + offset.Ticks, DateTime.MinValue.Ticks, DateTime.MaxValue.Ticks), DateTimeKind.Utc);

    private static JwtVerificationKey? VerifySignature(string raw, int first, int last, JwtValidationOptions options, JwtMetadata? metadata, out bool refresh)
    {
        refresh = false;
        var (algorithm, kid, thumbprint) = ReadHeader(raw.AsSpan(0, first));
        if (algorithm == null)
            return null;

        var key = ResolveKey(options.SigningKey, metadata, kid, thumbprint);
        var input = GC.AllocateUninitializedArray<byte>(last);
        _ = Encoding.ASCII.GetBytes(raw.AsSpan(0, last), input);
        var signature = Base64Url.DecodeFromChars(raw.AsSpan(last + 1));
        var verifiedKey = key ?? VerifyCandidates(input, signature, algorithm, options, metadata);
        if (key?.Verify(input, signature, algorithm) == false)
            verifiedKey = null;
        refresh = verifiedKey == null;
        return verifiedKey;
    }

    private static (string? Algorithm, string? KeyId, string? Thumbprint) ReadHeader(ReadOnlySpan<char> raw)
    {
        using var header = JsonDocument.Parse(Base64Url.DecodeFromChars(raw));
        var root = header.RootElement;
        if (!IsUniqueObject(root) || root.TryGetProperty("crit", out _) || root.TryGetProperty("b64", out _))
            return default;
        if (root.GetProperty("alg").GetString() is not { } algorithm)
            return default;

        var kid = root.TryGetProperty("kid", out var identifier) ? identifier.GetString() : null;
        var thumbprint = root.TryGetProperty("x5t", out var thumb) ? thumb.GetString() : null;
        return (algorithm, kid, thumbprint);
    }

    private static JwtVerificationKey? ResolveKey(JwtVerificationKey? configured, JwtMetadata? metadata, string? kid, string? thumbprint)
    {
        if (kid == null && thumbprint == null)
            return null;
        if (configured?.Matches(kid, thumbprint) == true)
            return configured;
        if (metadata != null)
        {
            for (var index = 0; index < metadata.SigningKeys.Count; index++)
            {
                var candidate = metadata.SigningKeys[index];
                if (candidate.Matches(kid, thumbprint))
                    return candidate;
            }
        }

        return null;
    }

    private static JwtVerificationKey? VerifyCandidates(ReadOnlySpan<byte> input, ReadOnlySpan<byte> signature, string algorithm, JwtValidationOptions options, JwtMetadata? metadata)
    {
        if (options.SigningKey is { } configured && configured.Verify(input, signature, algorithm))
            return configured;
        if (metadata != null)
        {
            for (var index = 0; index < metadata.SigningKeys.Count; index++)
            {
                var candidate = metadata.SigningKeys[index];
                if (candidate.Verify(input, signature, algorithm))
                    return candidate;
            }
        }

        return null;
    }

    private static SignedConnectionToken? ValidateClaims(byte[] verified, JwtVerificationKey key, JwtValidationOptions options, JwtMetadata? metadata, DateTime now, out bool refresh)
    {
        refresh = false;
        using var payload = JsonDocument.Parse(verified);
        var root = payload.RootElement;
        if (!IsUniqueObject(root))
            return null;
        var issuer = root.GetProperty("iss").GetString();
        if (issuer == null || !MatchesAudience(root.GetProperty("aud"), options.Audience))
            return null;
        if (!options.AcceptsIssuer(issuer, metadata))
        {
            refresh = true;
            return null;
        }

        var upper = ReadDate(root.GetProperty("exp"));
        var lower = root.TryGetProperty("nbf", out var nbf) ? ReadDate(nbf) : DateTime.MinValue;
        if (upper < lower)
            return null;
        if (key.NotAfter is { } keyUpper && keyUpper < upper)
            upper = keyUpper;
        if (key.NotBefore is { } keyLower && keyLower > lower)
            lower = keyLower;
        upper = Offset(upper, ClockSkew);
        lower = Offset(lower, -ClockSkew);
        return now < lower || now >= upper ? null : new SignedConnectionToken(issuer, key, CreatePrincipal(root, issuer), lower, upper);
    }

    private static DateTime ReadDate(JsonElement value)
    {
        var seconds = Math.Max(0, ReadSeconds(value));
        return seconds > DateTimeOffset.MaxValue.ToUnixTimeSeconds()
            ? DateTime.SpecifyKind(DateTime.MaxValue, DateTimeKind.Utc)
            : DateTimeOffset.FromUnixTimeSeconds(seconds).UtcDateTime;
    }

    private static long ReadSeconds(JsonElement value) =>
        value.ValueKind switch
        {
            JsonValueKind.String => long.Parse(value.GetString()!, NumberStyles.Integer, CultureInfo.InvariantCulture),
            JsonValueKind.Number when value.TryGetInt64(out var seconds) => seconds,
            JsonValueKind.Number => Convert.ToInt64(value.GetDouble()),
            _ => throw new FormatException("JWT NumericDate must contain seconds."),
        };

    private static ClaimsPrincipal CreatePrincipal(JsonElement root, string issuer)
    {
        var identity = new ClaimsIdentity(null, null, "AuthenticationTypes.Federation", ClaimTypes.Name, ClaimTypes.Role, StringComparison.Ordinal);
        foreach (var property in root.EnumerateObject())
        {
            if (property.Value.ValueKind == JsonValueKind.Array)
            {
                foreach (var item in property.Value.EnumerateArray())
                    AddClaim(identity, property.Name, item, issuer);
            }
            else
            {
                AddClaim(identity, property.Name, property.Value, issuer);
            }
        }

        return new ClaimsPrincipal(identity);
    }

    private static bool IsUniqueObject(JsonElement element)
    {
        if (element.ValueKind != JsonValueKind.Object)
            return false;
        var names = new HashSet<string>(StringComparer.Ordinal);
        foreach (var property in element.EnumerateObject())
        {
            if (!names.Add(property.Name))
                return false;
        }

        return true;
    }

    private static bool MatchesAudience(JsonElement audience, string? expected)
    {
        if (string.IsNullOrWhiteSpace(expected))
            return false;
        if (audience.ValueKind == JsonValueKind.String)
            return SameAudience(audience.GetString(), expected);
        if (audience.ValueKind != JsonValueKind.Array)
            return false;
        var match = false;
        foreach (var item in audience.EnumerateArray())
            match |= SameAudience(item.GetString(), expected);
        return match;
    }

    private static bool SameAudience(string? actual, string expected) =>
        string.Equals(actual, expected, StringComparison.Ordinal) ||
        (actual != null && ((actual.Length == expected.Length + 1 && actual[^1] == '/' && actual.AsSpan(0, expected.Length).SequenceEqual(expected)) ||
        (expected.Length == actual.Length + 1 && expected[^1] == '/' && expected.AsSpan(0, actual.Length).SequenceEqual(actual))));

    private static void AddClaim(ClaimsIdentity identity, string name, JsonElement value, string issuer)
    {
        if (string.Equals(name, "exp", StringComparison.Ordinal) || string.Equals(name, "iat", StringComparison.Ordinal) || string.Equals(name, "nbf", StringComparison.Ordinal))
        {
            identity.AddClaim(new Claim(name, ReadSeconds(value).ToString(CultureInfo.InvariantCulture), ClaimValueTypes.Integer64, issuer, issuer, identity));
            return;
        }

        var mapped = ConnectionClaimTypes.Map(name);
        var type = value.ValueKind switch
        {
            JsonValueKind.String => ClaimValueTypes.String,
            JsonValueKind.Number when value.TryGetInt32(out _) => ClaimValueTypes.Integer32,
            JsonValueKind.Number when value.TryGetInt64(out _) => ClaimValueTypes.Integer64,
            JsonValueKind.Number => ClaimValueTypes.Double,
            JsonValueKind.True or JsonValueKind.False => ClaimValueTypes.Boolean,
            JsonValueKind.Array => "JSON_ARRAY",
            JsonValueKind.Object => "JSON",
            _ => "JSON_NULL",
        };
        var text = value.ValueKind switch
        {
            JsonValueKind.String => value.GetString()!,
            JsonValueKind.Null => string.Empty,
            _ => value.GetRawText(),
        };
        var claim = new Claim(mapped, text, type, issuer, issuer, identity);
        if (!string.Equals(mapped, name, StringComparison.Ordinal))
            claim.Properties.Add("http://schemas.xmlsoap.org/ws/2005/05/identity/claimproperties/ShortTypeName", name);
        identity.AddClaim(claim);
    }
}
