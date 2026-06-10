using System;
using System.Collections.Generic;
using System.Text;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Node.Hosting.Security;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixSecurityServiceRegistration
{
    public static bool AddSquirixSecurityServices(this IServiceCollection services, SecurityOptions? securityOptionsOverride = null)
    {
        var (apiKeySet, jwtAuthority, jwtAudience, jwtIssuer, jwtAllowHttpMetadata, signingKeyBytes, jwtEnabled) = ResolveSecurityConfiguration(securityOptionsOverride);

        if (!string.IsNullOrWhiteSpace(jwtIssuer) && signingKeyBytes is null && string.IsNullOrWhiteSpace(jwtAuthority))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER requires SQUIRIX_JWT_SIGNING_KEY when no authority is configured.");

        var authEnabled = apiKeySet.Count > 0 || jwtEnabled;
        if (!authEnabled)
            return false;

        var authBuilder = services.AddAuthentication();

        if (apiKeySet.Count > 0)
        {
            _ = services.AddSingleton(new ApiKeyAuthSettings(apiKeySet));
            _ = authBuilder.AddScheme<AuthenticationSchemeOptions, ApiKeyAuthenticationHandler>(ApiKeyAuthenticationDefaults.Scheme, static _ => { });
        }

        if (jwtEnabled)
        {
            if (string.IsNullOrWhiteSpace(jwtAuthority) && signingKeyBytes is null)
                throw new InvalidOperationException("JWT authentication requires SQUIRIX_JWT_AUTHORITY or SQUIRIX_JWT_SIGNING_KEY.");

            if (string.IsNullOrWhiteSpace(jwtAuthority) && string.IsNullOrWhiteSpace(jwtIssuer))
                throw new InvalidOperationException("SQUIRIX_JWT_ISSUER must be provided when using SQUIRIX_JWT_SIGNING_KEY without SQUIRIX_JWT_AUTHORITY.");

            _ = authBuilder.AddJwtBearer(
                JwtBearerDefaults.AuthenticationScheme,
                o =>
                {
                    var hasAuthority = !string.IsNullOrWhiteSpace(jwtAuthority);
                    o.Authority = hasAuthority ? jwtAuthority : null;
                    o.RequireHttpsMetadata = hasAuthority && !jwtAllowHttpMetadata;

                    if (!string.IsNullOrWhiteSpace(jwtAudience))
                        o.Audience = jwtAudience;

                    var parameters = new TokenValidationParameters
                    {
                        ValidateAudience = !string.IsNullOrWhiteSpace(jwtAudience),
                        ValidateLifetime = true,
                        ValidateIssuerSigningKey = true,
                        ClockSkew = TimeSpan.FromMinutes(2),
                    };

                    if (!string.IsNullOrWhiteSpace(jwtAuthority))
                    {
                        parameters.ValidateIssuer = true;
                        if (!string.IsNullOrWhiteSpace(jwtIssuer))
                            parameters.ValidIssuer = jwtIssuer;
                    }
                    else
                    {
                        parameters.ValidateIssuer = true;
                        parameters.ValidIssuer = jwtIssuer;
                    }

                    if (!string.IsNullOrWhiteSpace(jwtAudience))
                        parameters.ValidAudience = jwtAudience;

                    if (signingKeyBytes is not null)
                        parameters.IssuerSigningKey = new SymmetricSecurityKey(signingKeyBytes);

                    o.TokenValidationParameters = parameters;
                });
        }

        _ = services.AddAuthorizationBuilder().AddPolicy(
            "ApiOrJwt",
            p =>
            {
                _ = p.RequireAuthenticatedUser();
                switch (apiKeySet.Count)
                {
                    case > 0 when jwtEnabled:
                        _ = p.AddAuthenticationSchemes(ApiKeyAuthenticationDefaults.Scheme, JwtBearerDefaults.AuthenticationScheme);
                        break;

                    case > 0:
                        _ = p.AddAuthenticationSchemes(ApiKeyAuthenticationDefaults.Scheme);
                        break;

                    default:
                    {
                        if (jwtEnabled)
                            _ = p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme);
                        break;
                    }
                }
            });

        return true;
    }

    private static ResolvedSecurityConfiguration ResolveSecurityConfiguration(SecurityOptions? securityOptionsOverride)
    {
        if (securityOptionsOverride is null)
            return ResolveFromEnvironment();

        var apiKeySet = new HashSet<string>(StringComparer.Ordinal);
        if (securityOptionsOverride.ApiKeys is not null)
        {
            foreach (var key in securityOptionsOverride.ApiKeys)
            {
                if (!string.IsNullOrWhiteSpace(key))
                    _ = apiKeySet.Add(key.Trim());
            }
        }

        var jwtAuthority = string.Empty;
        var jwtAudience = securityOptionsOverride.JwtAudience ?? string.Empty;
        var jwtIssuer = securityOptionsOverride.JwtIssuer ?? string.Empty;
        var jwtSigningKey = securityOptionsOverride.JwtSigningKey ?? string.Empty;
        var signingKeyBytes = TryDecodeSymmetricKey(jwtSigningKey);
        var jwtEnabled = signingKeyBytes is not null;

        return new ResolvedSecurityConfiguration(apiKeySet, jwtAuthority, jwtAudience, jwtIssuer, false, signingKeyBytes, jwtEnabled);
    }

    private static ResolvedSecurityConfiguration ResolveFromEnvironment()
    {
        var apiKeysEnv = EnvVariables.ReadStringOrEmpty("SQUIRIX_API_KEYS");
        var apiKeySet = new HashSet<string>(apiKeysEnv.Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries), StringComparer.Ordinal);

        var jwtAuthority = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUTHORITY");
        var jwtAudience = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUDIENCE");
        var jwtIssuer = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_ISSUER");
        var jwtSigningKey = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_SIGNING_KEY");
        var jwtAllowHttpMetadata = EnvVariables.ReadBool("SQUIRIX_JWT_ALLOW_HTTP_METADATA");
        var signingKeyBytes = TryDecodeSymmetricKey(jwtSigningKey);
        var jwtEnabled = !string.IsNullOrWhiteSpace(jwtAuthority) || signingKeyBytes is not null;

        return new ResolvedSecurityConfiguration(apiKeySet, jwtAuthority, jwtAudience, jwtIssuer, jwtAllowHttpMetadata, signingKeyBytes, jwtEnabled);
    }

    private static byte[]? TryDecodeSymmetricKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        try
        {
            return Convert.FromBase64String(value);
        }
        catch (FormatException)
        {
            return Encoding.UTF8.GetBytes(value);
        }
    }

    private readonly record struct ResolvedSecurityConfiguration(
        HashSet<string> ApiKeySet,
        string JwtAuthority,
        string JwtAudience,
        string JwtIssuer,
        bool JwtAllowHttpMetadata,
        byte[]? SigningKeyBytes,
        bool JwtEnabled);
}
