using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Claims;
using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.Server.TestKit;

/// <summary>JWT helpers for in-process server and integration tests.</summary>
public static class TestJwtHelper
{
    /// <summary>Writes a bearer token for the supplied credentials.</summary>
    /// <param name="credentials">Signing material and claim values.</param>
    /// <param name="lifetime">Optional token lifetime; defaults to five minutes.</param>
    /// <param name="subject">Optional <c language="csharp">sub</c> claim value; the token carries no subject when <see langword="null" />.</param>
    /// <returns>A compact JWT bearer token string.</returns>
    public static string CreateBearerToken(TestJwtCredentials credentials, TimeSpan? lifetime = null, string? subject = null)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        var signingCredentials = new SigningCredentials(new SymmetricSecurityKey(credentials.GetSigningKey()), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var expires = now.Add(lifetime ?? TimeSpan.FromMinutes(5));
        Claim[]? claims = subject == null ? null : [new Claim(JwtRegisteredClaimNames.Sub, subject)];
        var token = new JwtSecurityToken(credentials.Issuer, credentials.Audience, claims, now.AddMinutes(-1), expires, signingCredentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
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
}
