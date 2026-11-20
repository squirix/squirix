using System;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class SecurityServiceRegistration
{
    /// <summary>Minimum symmetric key length: HMAC-SHA256 keys shorter than the hash output weaken the signature.</summary>
    private const int MinSigningKeyBytes = 32;

    internal static bool AddSquirixSecurityServices(this IServiceCollection services, SecurityOptions? securityOptionsOverride = null)
    {
        var configuration = ResolveSecurityConfiguration(securityOptionsOverride);
        ValidateSecurityConfiguration(in configuration);

        if (!configuration.JwtEnabled)
            return false;

        RegisterNativeAuthentication(services, in configuration);
        return true;
    }

    private static TokenValidationParameters CreateTokenValidationParameters(in ResolvedSecurityConfiguration configuration)
    {
        var parameters = new TokenValidationParameters
        {
            ValidateAudience = true,
            ValidAudience = configuration.JwtAudience,
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

        if (configuration.SigningKeyBytes != null)
            parameters.IssuerSigningKey = new SymmetricSecurityKey(configuration.SigningKeyBytes);

        return parameters;
    }

    private static byte[]? DecodeSymmetricKey(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
            return null;

        byte[] key;
        try
        {
            key = Convert.FromBase64String(value);
        }
        catch (FormatException ex)
        {
            throw new InvalidOperationException("SQUIRIX_JWT_SIGNING_KEY must be base64.", ex);
        }

        return key.Length >= MinSigningKeyBytes
            ? key
            : throw new InvalidOperationException($"SQUIRIX_JWT_SIGNING_KEY must decode to at least {MinSigningKeyBytes} bytes.");
    }

    private static void RegisterNativeAuthentication(IServiceCollection services, in ResolvedSecurityConfiguration configuration)
    {
        var requireHttps = !string.IsNullOrWhiteSpace(configuration.JwtAuthority) && !configuration.JwtAllowHttpMetadata;
        var metadata = string.IsNullOrWhiteSpace(configuration.JwtAuthority)
            ? null
            : new ConfigurationManager<OpenIdConnectConfiguration>(
                $"{configuration.JwtAuthority.TrimEnd('/')}/.well-known/openid-configuration",
                new OpenIdConnectConfigurationRetriever(),
                new HttpDocumentRetriever { RequireHttps = requireHttps });
        var parameters = CreateTokenValidationParameters(in configuration);
        _ = services.AddOptions<JwtValidationOptions>().Configure(options =>
        {
            options.TokenValidationParameters = parameters;
            options.ConfigurationManager = metadata;
            options.RequireHttpsMetadata = requireHttps;
        });
        _ = services.AddSingleton(static provider => new NativeConnectionAuthenticationValidator(
            provider.GetRequiredService<IOptionsMonitor<JwtValidationOptions>>(), TimeProvider.System, provider.GetRequiredService<IServiceScopeFactory>()));
    }

    private static ResolvedSecurityConfiguration ResolveFromEnvironment()
    {
        var jwtAuthority = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUTHORITY");
        var jwtAudience = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_AUDIENCE");
        var jwtIssuer = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_ISSUER");
        var jwtSigningKey = EnvVariables.ReadStringOrEmpty("SQUIRIX_JWT_SIGNING_KEY");
        var jwtAllowHttpMetadata = EnvVariables.ReadBool("SQUIRIX_JWT_ALLOW_HTTP_METADATA");
        var signingKeyBytes = DecodeSymmetricKey(jwtSigningKey);
        return new ResolvedSecurityConfiguration(jwtAuthority, jwtAudience, jwtIssuer, jwtAllowHttpMetadata, signingKeyBytes);
    }

    private static ResolvedSecurityConfiguration ResolveSecurityConfiguration(SecurityOptions? securityOptionsOverride)
    {
        if (securityOptionsOverride == null)
            return ResolveFromEnvironment();

        var jwtAuthority = securityOptionsOverride.JwtAuthority ?? string.Empty;
        var jwtAudience = securityOptionsOverride.JwtAudience ?? string.Empty;
        var jwtIssuer = securityOptionsOverride.JwtIssuer ?? string.Empty;
        var jwtSigningKey = securityOptionsOverride.JwtSigningKey ?? string.Empty;
        var signingKeyBytes = DecodeSymmetricKey(jwtSigningKey);
        return new ResolvedSecurityConfiguration(jwtAuthority, jwtAudience, jwtIssuer, securityOptionsOverride.JwtAllowHttpMetadata, signingKeyBytes);
    }

    private static void ValidateSecurityConfiguration(in ResolvedSecurityConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.JwtIssuer) && configuration.SigningKeyBytes == null && string.IsNullOrWhiteSpace(configuration.JwtAuthority))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER requires SQUIRIX_JWT_SIGNING_KEY when no authority is configured.");

        ValidateAudience(in configuration);

        if (!configuration.JwtEnabled)
            return;

        if (string.IsNullOrWhiteSpace(configuration.JwtAuthority) && string.IsNullOrWhiteSpace(configuration.JwtIssuer))
            throw new InvalidOperationException("SQUIRIX_JWT_ISSUER must be provided when using SQUIRIX_JWT_SIGNING_KEY without SQUIRIX_JWT_AUTHORITY.");
    }

    private static void ValidateAudience(in ResolvedSecurityConfiguration configuration)
    {
        if (!string.IsNullOrWhiteSpace(configuration.JwtAudience))
            return;

        if (!string.IsNullOrWhiteSpace(configuration.JwtAuthority))
            throw new InvalidOperationException("SQUIRIX_JWT_AUTHORITY requires SQUIRIX_JWT_AUDIENCE.");

        if (configuration.SigningKeyBytes != null)
            throw new InvalidOperationException("SQUIRIX_JWT_SIGNING_KEY requires SQUIRIX_JWT_AUDIENCE.");
    }

    [Immutable]
    private readonly record struct ResolvedSecurityConfiguration(
        string JwtAuthority,
        string JwtAudience,
        string JwtIssuer,
        bool JwtAllowHttpMetadata,
        byte[]? SigningKeyBytes)
    {
        internal bool JwtEnabled => !string.IsNullOrWhiteSpace(JwtAuthority) || SigningKeyBytes != null;
    }
}
