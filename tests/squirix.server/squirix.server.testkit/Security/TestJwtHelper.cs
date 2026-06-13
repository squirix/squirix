using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.TestKit.AspNetCore;

namespace Squirix.Server.TestKit.Security;

/// <summary>
/// JWT helpers for in-process server and integration tests.
/// </summary>
public static class TestJwtHelper
{
    /// <summary>Writes a bearer token for the supplied credentials.</summary>
    /// <param name="credentials">Signing material and claim values.</param>
    /// <param name="lifetime">Optional token lifetime; defaults to five minutes.</param>
    /// <returns>A compact JWT bearer token string.</returns>
    public static string CreateBearerToken(TestJwtCredentials credentials, TimeSpan? lifetime = null)
    {
        ArgumentNullException.ThrowIfNull(credentials);
        var signingCredentials = new SigningCredentials(new SymmetricSecurityKey(credentials.GetSigningKey()), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var expires = now.Add(lifetime ?? TimeSpan.FromMinutes(5));
        var token = new JwtSecurityToken(credentials.Issuer, credentials.Audience, notBefore: now.AddMinutes(-1), expires: expires, signingCredentials: signingCredentials);
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

    /// <summary>Maps credentials to node security options.</summary>
    /// <param name="credentials">Symmetric JWT credentials.</param>
    /// <returns>Per-node security override for in-process test hosts.</returns>
    public static TestNodeSecurityOptions ToSecurityOptions(TestJwtCredentials credentials)
    {
        ArgumentNullException.ThrowIfNull(credentials);

        return new TestNodeSecurityOptions
        {
            JwtSigningKey = credentials.Base64SigningKey,
            JwtIssuer = credentials.Issuer,
            JwtAudience = credentials.Audience,
        };
    }
}
