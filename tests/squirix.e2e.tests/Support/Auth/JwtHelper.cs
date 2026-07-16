using System;
using System.IdentityModel.Tokens.Jwt;
using System.Security.Cryptography;
using Microsoft.IdentityModel.Tokens;

namespace Squirix.E2ETests.Support.Auth;

/// <summary>Creates JWT credentials for authenticated E2E scenarios.</summary>
internal static class JwtHelper
{
    public static JwtCredentials CreateSymmetricCredentials()
    {
        var signingKey = RandomNumberGenerator.GetBytes(32);
        const string issuer = "https://e2e.squirix.test";
        const string audience = "squirix-e2e";
        return new JwtCredentials(signingKey, Convert.ToBase64String(signingKey), issuer, audience);
    }

    internal static string CreateBearerToken(JwtCredentials credentials)
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
}
