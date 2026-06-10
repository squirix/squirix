using System;
using System.Text;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class SquirixSecurityServiceRegistration
{
    public const string JwtBearerPolicy = "JwtBearer";

    public static bool AddSquirixSecurityServices(this IServiceCollection services, SecurityOptions? securityOptionsOverride = null)
    {
        var (jwtAuthority, jwtAudience, jwtIssuer, jwtAllowHttpMetadata, signingKeyBytes, jwtEnabled) = ResolveSecurityConfiguration(securityOptionsOverride);

        if (!string.IsNullOrWhiteSpace(jwtIssuer) && signingKeyBytes is null && string.IsNullOrWhiteSpace(jwtAuthority))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER requires SQUIRIX_JWT_SIGNING_KEY when no authority is configured.");

        if (!jwtEnabled)
            return false;

        if (string.IsNullOrWhiteSpace(jwtAuthority) && signingKeyBytes is null)
            throw new InvalidOperationException("JWT authentication requires SQUIRIX_JWT_AUTHORITY or SQUIRIX_JWT_SIGNING_KEY.");

        if (string.IsNullOrWhiteSpace(jwtAuthority) && string.IsNullOrWhiteSpace(jwtIssuer))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER must be provided when using SQUIRIX_JWT_SIGNING_KEY without SQUIRIX_JWT_AUTHORITY.");

        _ = services.AddAuthentication()
            .AddJwtBearer(
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

        _ = services.AddAuthorizationBuilder().AddPolicy(
            JwtBearerPolicy,
            p =>
            {
                _ = p.RequireAuthenticatedUser();
                _ = p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme);
            });

        return true;
    }

    private static ResolvedSecurityConfiguration ResolveSecurityConfiguration(SecurityOptions? securityOptionsOverride)
    {
        if (securityOptionsOverride is null)
            return ResolveFromEnvironment();

        var jwtAuthority = string.Empty;
        var jwtAudience = securityOptionsOverride.JwtAudience ?? string.Empty;
        var jwtIssuer = securityOptionsOverride.JwtIssuer ?? string.Empty;
        var jwtSigningKey = securityOptionsOverride.JwtSigningKey ?? string.Empty;
        var signingKeyBytes = TryDecodeSymmetricKey(jwtSigningKey);
        var jwtEnabled = signingKeyBytes is not null;

        return new ResolvedSecurityConfiguration(jwtAuthority, jwtAudience, jwtIssuer, false, signingKeyBytes, jwtEnabled);
    }

    private static ResolvedSecurityConfiguration ResolveFromEnvironment()
    {
        var jwtAuthority = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUTHORITY");
        var jwtAudience = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUDIENCE");
        var jwtIssuer = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_ISSUER");
        var jwtSigningKey = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_SIGNING_KEY");
        var jwtAllowHttpMetadata = EnvVariables.ReadBool("SQUIRIX_JWT_ALLOW_HTTP_METADATA");
        var signingKeyBytes = TryDecodeSymmetricKey(jwtSigningKey);
        var jwtEnabled = !string.IsNullOrWhiteSpace(jwtAuthority) || signingKeyBytes is not null;

        return new ResolvedSecurityConfiguration(jwtAuthority, jwtAudience, jwtIssuer, jwtAllowHttpMetadata, signingKeyBytes, jwtEnabled);
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
        string JwtAuthority,
        string JwtAudience,
        string JwtIssuer,
        bool JwtAllowHttpMetadata,
        byte[]? SigningKeyBytes,
        bool JwtEnabled);
}
