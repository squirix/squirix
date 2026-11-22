using System;
using System.Buffers.Text;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace Squirix.Server.TestKit;

/// <summary>JWT helpers for in-process server and integration tests.</summary>
public static partial class TestJwtHelper
{
    /// <summary>Writes a bearer token for the supplied credentials.</summary>
    /// <param name="credentials">Signing material and claim values.</param>
    /// <param name="subject">Optional <c language="csharp">sub</c> claim value; the token carries no subject when <see langword="null" />.</param>
    /// <returns>A compact JWT bearer token string.</returns>
    /// <exception cref="ArgumentException">The signing key is empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The signing key is shorter than 32 bytes.</exception>
    public static string CreateBearerToken(TestJwtCredentials credentials, string? subject = null)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        var signingKey = credentials.GetSigningKey();
        if (signingKey.Length == 0)
            throw new ArgumentException("The signing key must not be empty.", nameof(credentials));
        if (signingKey.Length < SHA256.HashSizeInBytes)
            throw new ArgumentOutOfRangeException(nameof(credentials), "HS256 requires a signing key of at least 32 bytes.");
        var now = DateTimeOffset.UtcNow;
        var input = CreateSigningInput("HS256", credentials.Issuer, credentials.Audience, now.AddMinutes(-1), now.AddMinutes(5), subject);
        var signature = HMACSHA256.HashData(signingKey, Encoding.ASCII.GetBytes(input));
        return $"{input}.{Base64Url.EncodeToString(signature)}";
    }

    /// <summary>Creates random symmetric JWT credentials.</summary>
    /// <param name="issuer">JWT issuer claim value.</param>
    /// <param name="audience">JWT audience claim value.</param>
    /// <returns>Fresh symmetric credentials for a test node and its callers.</returns>
    public static TestJwtCredentials CreateRandomCredentials(string issuer = "https://test.squirix.dev", string audience = "squirix-test")
    {
        var signingKey = RandomNumberGenerator.GetBytes(32);
        return new TestJwtCredentials(signingKey, issuer, audience);
    }

    internal static string CreateSigningInput(
        string algorithm,
        string issuer,
        string audience,
        DateTimeOffset notBefore,
        DateTimeOffset expires,
        string? subject = null,
        string? keyId = null)
    {
        var header = JsonSerializer.SerializeToUtf8Bytes(new Header(algorithm, keyId, "JWT"), JwtJsonContext.Default.Header);
        var claims = new Claims(
            subject,
            notBefore.ToUnixTimeSeconds(),
            expires.ToUnixTimeSeconds(),
            string.IsNullOrEmpty(issuer) ? null : issuer,
            string.IsNullOrEmpty(audience) ? null : audience);
        var payload = JsonSerializer.SerializeToUtf8Bytes(claims, JwtJsonContext.Default.Claims);
        return $"{Base64Url.EncodeToString(header)}.{Base64Url.EncodeToString(payload)}";
    }

    [JsonSourceGenerationOptions(
        PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        RespectNullableAnnotations = true,
        RespectRequiredConstructorParameters = true)]
    [JsonSerializable(typeof(Header))]
    [JsonSerializable(typeof(Claims))]
    private sealed partial class JwtJsonContext : JsonSerializerContext;

    private sealed record Header(string Alg, string? Kid, string Typ);

    private sealed record Claims(string? Sub, long Nbf, long Exp, string? Iss, string? Aud);
}
