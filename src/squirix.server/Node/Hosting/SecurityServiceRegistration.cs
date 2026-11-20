using System;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Composition;
using Squirix.Server.Utils;

namespace Squirix.Server.Node.Hosting;

internal static class SecurityServiceRegistration
{
    /// <summary>Minimum symmetric key length: HMAC-SHA256 keys shorter than the hash output weaken the signature.</summary>
    private const int MinSigningKeyBytes = 32;

    internal static bool AddSquirixSecurityServices(this NodeServices services, SecurityOptions? securityOptionsOverride = null)
    {
        var configuration = ResolveSecurityConfiguration(securityOptionsOverride);
        ValidateSecurityConfiguration(in configuration);

        if (!configuration.JwtEnabled)
            return false;

        RegisterNativeAuthentication(services, in configuration);
        return true;
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

    private static void RegisterNativeAuthentication(NodeServices services, in ResolvedSecurityConfiguration configuration)
    {
        var requireHttps = !string.IsNullOrWhiteSpace(configuration.JwtAuthority) && !configuration.JwtAllowHttpMetadata;
        var metadata = string.IsNullOrWhiteSpace(configuration.JwtAuthority)
            ? null
            : new JwtMetadataSource($"{configuration.JwtAuthority.TrimEnd('/')}/.well-known/openid-configuration", requireHttps);
        var options = new JwtValidationOptions
        {
            Audience = configuration.JwtAudience,
            Issuer = string.IsNullOrWhiteSpace(configuration.JwtIssuer) ? null : configuration.JwtIssuer,
            SigningKey = configuration.SigningKeyBytes == null ? null : new JwtVerificationKey(configuration.SigningKeyBytes),
            RequireHttpsMetadata = requireHttps,
            Metadata = metadata,
        };
        _ = services.Use<Func<JwtValidationOptions>>(() => options);
        _ = services.Own(static provider => new NativeConnectionAuthenticationValidator(
            provider.Require<Func<JwtValidationOptions>>(), TimeProvider.System, provider));
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
