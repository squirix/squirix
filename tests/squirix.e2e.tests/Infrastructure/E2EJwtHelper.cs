using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.E2ETests.Infrastructure;

/// <summary>
/// Creates JWT credentials for authenticated E2E scenarios.
/// </summary>
internal static class E2EJwtHelper
{
    public static string CreateBearerToken(E2EJwtCredentials credentials)
    {
        var signingCredentials = new SigningCredentials(new SymmetricSecurityKey(credentials.SigningKey), SecurityAlgorithms.HmacSha256);
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(
            credentials.Issuer,
            credentials.Audience,
            notBefore: now.AddMinutes(-1),
            expires: now.AddMinutes(5),
            signingCredentials: signingCredentials);
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    public static E2EJwtCredentials CreateSymmetricCredentials()
    {
        var signingKey = RandomNumberGenerator.GetBytes(32);
        const string issuer = "https://e2e.squirix.test";
        const string audience = "squirix-e2e";
        return new E2EJwtCredentials(signingKey, Convert.ToBase64String(signingKey), issuer, audience);
    }
}
