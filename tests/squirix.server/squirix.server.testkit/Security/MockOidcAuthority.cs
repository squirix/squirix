using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using JetBrains.Annotations;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Hosting;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.TestKit.AspNetCore;
using Squirix.Server.TestKit.Http;

namespace Squirix.Server.TestKit.Security;

/// <summary>
/// In-process OIDC authority that serves discovery metadata and JWKS for JWT bearer tests.
/// </summary>
public sealed class MockOidcAuthority : IAsyncDisposable
{
    private static readonly PortAllocator PortPool = new(48000, 48999);

    private readonly RSA _signingKey;
    private readonly WebApplication _app;
    private readonly string _keyId;

    private MockOidcAuthority(WebApplication app, string authorityUrl, string issuer, RSA signingKey, string keyId)
    {
        _app = app;
        AuthorityUrl = authorityUrl;
        Issuer = issuer;
        _signingKey = signingKey;
        _keyId = keyId;
    }

    /// <summary>Gets the authority base URL (also used as the token issuer).</summary>
    private string AuthorityUrl { get; }

    /// <summary>Gets the issuer claim value published in discovery metadata.</summary>
    private string Issuer { get; }

    /// <summary>Starts a loopback mock authority on an ephemeral HTTP port.</summary>
    /// <param name="cancellationToken">Cancellation token.</param>
    /// <returns>A started mock authority.</returns>
    public static async Task<MockOidcAuthority> StartAsync(CancellationToken cancellationToken = default)
    {
        var port = PortPool.Allocate();
        var authorityUrl = $"http://127.0.0.1:{port}";
        var signingKey = RSA.Create(2048);
        const string keyId = "mock-oidc-key";

        var jwk = JsonWebKeyConverter.ConvertFromRSASecurityKey(new RsaSecurityKey(signingKey.ExportParameters(true)) { KeyId = keyId });
        jwk.Use = "sig";
        jwk.Alg = SecurityAlgorithms.RsaSha256;

        var discovery = new OidcDiscoveryDocument
        {
            Issuer = authorityUrl,
            JwksUri = $"{authorityUrl}/.well-known/jwks",
        };

        var builder = WebApplication.CreateBuilder(
            new WebApplicationOptions
            {
                EnvironmentName = Environments.Development,
            });
        _ = builder.WebHost.UseUrls(authorityUrl);

        var app = builder.Build();
        var jwks = new JsonWebKeySet();
        jwks.Keys.Add(jwk);
        _ = app.MapGet("/.well-known/openid-configuration", () => Results.Json(discovery));
        _ = app.MapGet("/.well-known/jwks", () => Results.Json(jwks));

        await app.StartAsync(cancellationToken).ConfigureAwait(false);
        return new MockOidcAuthority(app, authorityUrl, authorityUrl, signingKey, keyId);
    }

    /// <summary>Issues a bearer token signed with the authority's RSA key.</summary>
    /// <param name="audience">JWT audience claim.</param>
    /// <param name="lifetime">Optional token lifetime; defaults to five minutes.</param>
    /// <returns>A compact JWT bearer token string.</returns>
    public string CreateBearerToken(string audience, TimeSpan? lifetime = null)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(audience);

        var credentials = new SigningCredentials(new RsaSecurityKey(_signingKey) { KeyId = _keyId }, SecurityAlgorithms.RsaSha256);
        var now = DateTime.UtcNow;
        var effectiveLifetime = lifetime ?? TimeSpan.FromMinutes(5);
        var expires = now.Add(effectiveLifetime);
        var notBefore = effectiveLifetime < TimeSpan.Zero ? expires.AddMinutes(-1) : now.AddMinutes(-1);
        var token = new JwtSecurityToken(Issuer, audience, notBefore: notBefore, expires: expires, signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    /// <summary>Maps the mock authority to per-node security options.</summary>
    /// <param name="audience">JWT audience validation value.</param>
    /// <returns>Per-node security override for in-process test hosts.</returns>
    public TestNodeSecurityOptions ToSecurityOptions(string audience) => new()
    {
        JwtAuthority = AuthorityUrl,
        JwtIssuer = Issuer,
        JwtAudience = audience,
        JwtAllowHttpMetadata = true,
    };

    /// <inheritdoc />
    public async ValueTask DisposeAsync()
    {
        await _app.StopAsync().ConfigureAwait(false);
        await _app.DisposeAsync().ConfigureAwait(false);
        _signingKey.Dispose();
    }

    private sealed class OidcDiscoveryDocument
    {
        [JsonPropertyName("issuer")]
        public required string Issuer
        {
            [UsedImplicitly]
            get;
            init;
        }

        [JsonPropertyName("jwks_uri")]
        public required string JwksUri
        {
            [UsedImplicitly]
            get;
            init;
        }
    }
}
