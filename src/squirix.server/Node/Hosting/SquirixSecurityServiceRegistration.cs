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
        var configuration = ResolveSecurityConfiguration(securityOptionsOverride);
        ValidateSecurityConfiguration(configuration);

        if (!configuration.JwtEnabled)
            return false;

        RegisterJwtAuthentication(services, configuration);
        return true;
    }

    private static TokenValidationParameters CreateTokenValidationParameters(ResolvedSecurityConfiguration configuration)
    {
        var parameters = new TokenValidationParameters
        {
            ValidateAudience = !string.IsNullOrWhiteSpace(configuration.JwtAudience),
            ValidateLifetime = true,
            ValidateIssuerSigningKey = true,
            ClockSkew = TimeSpan.FromMinutes(2),
            ValidateIssuer = true,
        };

        if (!string.IsNullOrWhiteSpace(configuration.JwtAuthority))
        {
            if (!string.IsNullOrWhiteSpace(configuration.JwtIssuer))
                parameters.ValidIssuer = configuration.JwtIssuer;
        }
        else
        {
            parameters.ValidIssuer = configuration.JwtIssuer;
        }

        if (!string.IsNullOrWhiteSpace(configuration.JwtAudience))
            parameters.ValidAudience = configuration.JwtAudience;

        if (configuration.SigningKeyBytes is not null)
            parameters.IssuerSigningKey = new SymmetricSecurityKey(configuration.SigningKeyBytes);

        return parameters;
    }

    private static void RegisterJwtAuthentication(IServiceCollection services, ResolvedSecurityConfiguration configuration)
    {
        _ = services.AddAuthentication().AddJwtBearer(
            JwtBearerDefaults.AuthenticationScheme,
            o =>
            {
                var hasAuthority = !string.IsNullOrWhiteSpace(configuration.JwtAuthority);
                o.Authority = hasAuthority ? configuration.JwtAuthority : null;
                o.RequireHttpsMetadata = hasAuthority && !configuration.JwtAllowHttpMetadata;

                if (!string.IsNullOrWhiteSpace(configuration.JwtAudience))
                    o.Audience = configuration.JwtAudience;

                o.TokenValidationParameters = CreateTokenValidationParameters(configuration);
            });

        _ = services.AddAuthorizationBuilder().AddPolicy(
            JwtBearerPolicy,
            p =>
            {
                _ = p.RequireAuthenticatedUser();
                _ = p.AddAuthenticationSchemes(JwtBearerDefaults.AuthenticationScheme);
            });
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

    private static ResolvedSecurityConfiguration ResolveSecurityConfiguration(SecurityOptions? securityOptionsOverride)
    {
        if (securityOptionsOverride is null)
            return ResolveFromEnvironment();

        var jwtAuthority = securityOptionsOverride.JwtAuthority ?? string.Empty;
        var jwtAudience = securityOptionsOverride.JwtAudience ?? string.Empty;
        var jwtIssuer = securityOptionsOverride.JwtIssuer ?? string.Empty;
        var jwtSigningKey = securityOptionsOverride.JwtSigningKey ?? string.Empty;
        var signingKeyBytes = TryDecodeSymmetricKey(jwtSigningKey);
        var jwtEnabled = !string.IsNullOrWhiteSpace(jwtAuthority) || signingKeyBytes is not null;

        return new ResolvedSecurityConfiguration(jwtAuthority, jwtAudience, jwtIssuer, securityOptionsOverride.JwtAllowHttpMetadata, signingKeyBytes, jwtEnabled);
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

    private static void ValidateSecurityConfiguration(ResolvedSecurityConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.JwtIssuer) && configuration.SigningKeyBytes is null && string.IsNullOrWhiteSpace(configuration.JwtAuthority))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER requires SQUIRIX_JWT_SIGNING_KEY when no authority is configured.");

        if (!string.IsNullOrWhiteSpace(configuration.JwtAuthority) && string.IsNullOrWhiteSpace(configuration.JwtAudience))
            throw new InvalidOperationException("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE.");

        if (!configuration.JwtEnabled)
            return;

        if (string.IsNullOrWhiteSpace(configuration.JwtAuthority) && configuration.SigningKeyBytes is null)
            throw new InvalidOperationException("JWT authentication requires SQUIRIX_JWT_AUTHORITY or SQUIRIX_JWT_SIGNING_KEY.");

        if (string.IsNullOrWhiteSpace(configuration.JwtAuthority) && string.IsNullOrWhiteSpace(configuration.JwtIssuer))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER must be provided when using SQUIRIX_JWT_SIGNING_KEY without SQUIRIX_JWT_AUTHORITY.");
    }

    private readonly record struct ResolvedSecurityConfiguration(
        string JwtAuthority,
        string JwtAudience,
        string JwtIssuer,
        bool JwtAllowHttpMetadata,
        byte[]? SigningKeyBytes,
        bool JwtEnabled);
}
