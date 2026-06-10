using System;
using System.IdentityModel.Tokens.Jwt;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Net.Http.Json;
using System.Security.Cryptography;
using System.Threading.Tasks;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Cluster.Membership;
using Squirix.Server.TestKit.AspNetCore;
using Xunit;

namespace Squirix.Server.SmokeTests.Security;

/// <summary>
/// Smoke tests verifying that JWT bearer tokens protect cache endpoints when configured.
/// </summary>
public sealed class JwtAuthSmokeTests : SmokeTestBase
{
    /// <summary>Ensures REST cache endpoints reject requests without a bearer token when JWT auth is configured.</summary>
    /// <returns>A <see cref="Task" /> representing the asynchronous smoke test.</returns>
    [Fact]
    public async Task CacheEndpointsRequireJwtWhenConfigured()
    {
        var keyBytes = RandomNumberGenerator.GetBytes(32);
        var base64Key = Convert.ToBase64String(keyBytes);
        const string issuer = "https://smoke.squirix.test";
        const string audience = "smoke-cache";

        var url = GetNextHttpUrl();
        var peers = new[] { new Peer { NodeId = "node-jwt", Url = url } };

        await using var node = await StartNodeAsync(
            url,
            peers,
            security: new TestNodeSecurityOptions
            {
                JwtSigningKey = base64Key,
                JwtIssuer = issuer,
                JwtAudience = audience,
            },
            extraScope: Guid.NewGuid().ToString("N"),
            cancellationToken: DefaultCancellationToken);

        var entry = new CacheEntry<string> { Value = "ok", Version = 1 };

        var unauthorized = await HttpClient.PutAsJsonAsync($"{url}/api/v1/cache/jwt-smoke", entry, DefaultCancellationToken);
        Assert.Equal(HttpStatusCode.Unauthorized, unauthorized.StatusCode);

        using var request = new HttpRequestMessage(HttpMethod.Put, $"{url}/api/v1/cache/jwt-smoke");
        request.Content = JsonContent.Create(entry);
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", CreateJwt(keyBytes, issuer, audience));
        request.Version = HttpVersion.Version20;
        request.VersionPolicy = HttpVersionPolicy.RequestVersionExact;

        var authorized = await HttpClient.SendAsync(request, DefaultCancellationToken);
        Assert.True(authorized.IsSuccessStatusCode, $"Expected success with JWT, got {(int)authorized.StatusCode} {authorized.ReasonPhrase}");
    }

    private static string CreateJwt(byte[] signingKey, string issuer, string audience)
    {
        var credentials = new SigningCredentials(new SymmetricSecurityKey(signingKey), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(issuer, audience, notBefore: now.AddMinutes(-1), expires: now.AddMinutes(5), signingCredentials: credentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }
}
