using System;
using System.Buffers.Text;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Adapters.Endpoint;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks signatures produced by independent JWT issuers against the connection validator.</summary>
public sealed class SignedConnectionTokenTests
{
    /// <summary>Preserves JOSE signing families, identity mappings and ordinal claim-type matching.</summary>
    /// <param name="algorithm">Issuer signature algorithm.</param>
    [Test]
    [Arguments("HS256")]
    [Arguments("HS384")]
    [Arguments("HS512")]
    [Arguments("RS256")]
    [Arguments("RS384")]
    [Arguments("RS512")]
    [Arguments("PS256")]
    [Arguments("PS384")]
    [Arguments("PS512")]
    [Arguments("ES256")]
    [Arguments("ES384")]
    [Arguments("ES512")]
    public async Task AcceptsIssuerSignatureAndMapsClaims(string algorithm)
    {
        using var rsa = RSA.Create(2048);
        using var ec = ECDsa.Create(algorithm switch { "ES384" => ECCurve.NamedCurves.nistP384, "ES512" => ECCurve.NamedCurves.nistP521, _ => ECCurve.NamedCurves.nistP256 });
        SecurityKey issuerKey = algorithm[0] switch
        {
            'H' => new SymmetricSecurityKey(RandomNumberGenerator.GetBytes(64)),
            'E' => new ECDsaSecurityKey(ec),
            _ => new RsaSecurityKey(rsa),
        };
        var jwk = JsonWebKeyConverter.ConvertFromSecurityKey(issuerKey);
        var json = new JsonObject
        {
            ["kty"] = jwk.Kty,
            ["alg"] = algorithm,
            ["k"] = jwk.K,
            ["crv"] = jwk.Crv,
            ["x"] = jwk.X,
            ["y"] = jwk.Y,
            ["n"] = jwk.N,
            ["e"] = jwk.E,
        };
        using var document = JsonDocument.Parse(json.ToJsonString());
        var verificationKey = JwtVerificationKey.FromJwk(document.RootElement);
        var metadata = new JwtMetadata { Issuer = "issuer" };
        var options = new JwtValidationOptions { Audience = "cache" };
        if (issuerKey is SymmetricSecurityKey configured)
        {
            _ = await Assert.That(verificationKey).IsNull();
            verificationKey = new JwtVerificationKey(configured.Key);
            options.SigningKey = verificationKey;
        }
        else
        {
            metadata.SigningKeys.Add(verificationKey!);
        }

        var token = WriteToken(issuerKey, algorithm);
        var result = SignedConnectionToken.Verify(token, options, metadata, DateTime.UnixEpoch, out _);
        _ = await Assert.That(result).IsNotNull();
        await CheckInvalidSignaturesAsync(token, options, metadata);
        var principal = result!.Principal;
        _ = await Assert.That(principal.FindFirst(ClaimTypes.NameIdentifier)?.Value).IsEqualTo("subject");
        _ = await Assert.That(principal.IsInRole("operator")).IsTrue();
        _ = await Assert.That(principal.HasClaim("Tenant", "first")).IsTrue();
        _ = await Assert.That(principal.HasClaim("tenant", "first")).IsFalse();
        _ = await Assert.That(result.SigningKey).IsSameReferenceAs(verificationKey);
    }

    /// <summary>Selection respects a matching key identifier and permits fallback for an unrecognized identifier.</summary>
    /// <param name="designated">Whether an available key matches the token header.</param>
    /// <param name="thumbprint">Whether the token identifies its key with x5t.</param>
    [Test]
    [Arguments(false, false)]
    [Arguments(true, false)]
    [Arguments(false, true)]
    [Arguments(true, true)]
    public async Task SelectsDesignatedKeyBeforeFallback(bool designated, bool thumbprint)
    {
        var secret = RandomNumberGenerator.GetBytes(32);
        var signing = new SymmetricSecurityKey(secret) { KeyId = "a" };
        var metadata = new JwtMetadata { Issuer = "issuer" };
        metadata.SigningKeys.Add(new JwtVerificationKey(RandomNumberGenerator.GetBytes(32)) { KeyId = designated ? "a" : "c" });
        metadata.SigningKeys.Add(new JwtVerificationKey(secret) { KeyId = "b" });
        var options = new JwtValidationOptions { Audience = "cache" };
        var result = SignedConnectionToken.Verify(WriteToken(signing, "HS256", thumbprint), options, metadata, DateTime.UnixEpoch, out _);
        _ = await Assert.That(result != null).IsEqualTo(!designated);
    }

    /// <summary>A certificate can narrow both ends of the accepted JWT lifetime.</summary>
    [Test]
    public async Task CertificateCapsTokenLifetime()
    {
        using var rsa = RSA.Create(2048);
        var csr = new CertificateRequest("CN=squirix", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        var lower = DateTimeOffset.UnixEpoch.AddSeconds(5);
        var upper = lower.AddSeconds(10);
        using var certificate = csr.CreateSelfSigned(lower, upper);
        var json = new JsonObject { ["kty"] = "RSA", ["x5c"] = new JsonArray(Convert.ToBase64String(certificate.RawData)) };
        using var document = JsonDocument.Parse(json.ToJsonString());
        var metadata = new JwtMetadata { Issuer = "issuer" };
        metadata.SigningKeys.Add(JwtVerificationKey.FromJwk(document.RootElement)!);
        var options = new JwtValidationOptions { Audience = "cache" };
        var token = WriteToken(new RsaSecurityKey(rsa), "RS256");
        var result = SignedConnectionToken.Verify(token, options, metadata, DateTime.UnixEpoch, out _);
        _ = await Assert.That(result).IsNotNull();
        _ = await Assert.That(result!.NotBefore).IsEqualTo(lower.AddMinutes(-2).UtcDateTime);
        _ = await Assert.That(result.Expires).IsEqualTo(upper.AddMinutes(2).UtcDateTime);
        _ = await Assert.That(SignedConnectionToken.Verify(token, options, metadata, result.Expires, out _)).IsNull();
    }

    private static async Task CheckInvalidSignaturesAsync(string token, JwtValidationOptions options, JwtMetadata metadata)
    {
        var start = token.AsSpan().LastIndexOf('.') + 1;
        var signature = Base64Url.DecodeFromChars(token.AsSpan(start));
        signature[0] ^= 1;
        var invalid = string.Concat(token.AsSpan(0, start), Base64Url.EncodeToString(signature));
        _ = await Assert.That(SignedConnectionToken.Verify(invalid, options, metadata, DateTime.UnixEpoch, out _)).IsNull();
        var truncated = string.Concat(token.AsSpan(0, start), Base64Url.EncodeToString(signature.AsSpan(1)));
        _ = await Assert.That(SignedConnectionToken.Verify(truncated, options, metadata, DateTime.UnixEpoch, out _)).IsNull();
    }

    private static string WriteToken(SecurityKey key, string algorithm, bool thumbprint = false)
    {
        var credentials = new SigningCredentials(key, algorithm);
        Claim[] claims = [new Claim("sub", "subject"), new Claim("roles", "operator"), new Claim("Tenant", "first")];
        var signed = new JwtSecurityToken("issuer", "cache", claims, DateTime.UnixEpoch.AddSeconds(-10), DateTime.UnixEpoch.AddSeconds(60), credentials);
        if (thumbprint)
        {
            _ = signed.Header.Remove("kid");
            signed.Header.Add("x5t", key.KeyId);
        }

        return new JwtSecurityTokenHandler().WriteToken(signed);
    }
}
