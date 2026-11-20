using System;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Attributes;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies JWT mode selection from environment and explicit node configuration.</summary>
[Immutable]
[NotInParallel]
public sealed class SecurityServiceRegistrationTests
{
    private const string Audience = "squirix-test";
    private const string Authority = "https://authority.squirix.test";
    private const string Issuer = "https://issuer.squirix.test";

    /// <summary>Only an authority or signing key enables JWT, with the same options from either source.</summary>
    /// <param name="fromEnvironment">Whether configuration comes from the process environment.</param>
    /// <param name="caseIndex">The configuration case.</param>
    [Test]
    [Arguments(false, 0)]
    [Arguments(false, 1)]
    [Arguments(false, 2)]
    [Arguments(false, 3)]
    [Arguments(false, 4)]
    [Arguments(true, 0)]
    [Arguments(true, 1)]
    [Arguments(true, 2)]
    [Arguments(true, 3)]
    [Arguments(true, 4)]
    public async Task ConfigurationSelectsJwtMode(bool fromEnvironment, int caseIndex)
    {
        var security = CreateConfiguration(caseIndex);
        using var authority = new TempEnvironmentVariable("SQUIRIX_JWT_AUTHORITY", security.JwtAuthority);
        using var audience = new TempEnvironmentVariable("SQUIRIX_JWT_AUDIENCE", security.JwtAudience);
        using var issuer = new TempEnvironmentVariable("SQUIRIX_JWT_ISSUER", security.JwtIssuer);
        using var signingKey = new TempEnvironmentVariable("SQUIRIX_JWT_SIGNING_KEY", security.JwtSigningKey);
        using var metadata = new TempEnvironmentVariable("SQUIRIX_JWT_ALLOW_HTTP_METADATA", "false");
        var services = new ServiceCollection();

        var enabled = services.AddSquirixSecurityServices(fromEnvironment ? null : security);

        _ = await Assert.That(enabled).IsEqualTo(caseIndex >= 2);
        if (!enabled)
        {
            _ = await Assert.That(services.Count).IsEqualTo(0);
            return;
        }

        _ = services.AddLogging();
        await using var provider = services.BuildServiceProvider();
        var options = provider.GetRequiredService<IOptionsMonitor<JwtValidationOptions>>().CurrentValue;
        var parameters = options.TokenValidationParameters;
        var hasAuthority = caseIndex is 2 or 4;
        var metadataManager = options.ConfigurationManager as ConfigurationManager<OpenIdConnectConfiguration>;
        _ = await Assert.That(metadataManager?.MetadataAddress).IsEqualTo(hasAuthority ? $"{Authority}/.well-known/openid-configuration" : null);
        _ = await Assert.That(options.RequireHttpsMetadata).IsEqualTo(hasAuthority);
        _ = await Assert.That(parameters.ValidAudience).IsEqualTo(Audience);
        _ = await Assert.That(parameters.ValidIssuer).IsEqualTo(caseIndex >= 3 ? Issuer : null);
        _ = await Assert.That(parameters.ValidateAudience).IsTrue();
        _ = await Assert.That(parameters.ValidateIssuer).IsTrue();
        _ = await Assert.That(parameters.ValidateIssuerSigningKey).IsTrue();
        _ = await Assert.That(parameters.ValidateLifetime).IsTrue();
        if (caseIndex >= 3)
        {
            var key = await Assert.That(parameters.IssuerSigningKey).IsTypeOf<SymmetricSecurityKey>();
            _ = await Assert.That(Convert.ToBase64String(key!.Key)).IsEqualTo(security.JwtSigningKey);
        }
        else
        {
            _ = await Assert.That(parameters.IssuerSigningKey).IsNull();
        }
    }

    /// <summary>An explicit override replaces environment settings, including malformed signing keys.</summary>
    /// <param name="enableJwt">Whether the explicit configuration enables symmetric JWT.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OverrideReplacesInvalidEnvironment(bool enableJwt)
    {
        using var authority = new TempEnvironmentVariable("SQUIRIX_JWT_AUTHORITY", Authority);
        using var audience = new TempEnvironmentVariable("SQUIRIX_JWT_AUDIENCE", null);
        using var issuer = new TempEnvironmentVariable("SQUIRIX_JWT_ISSUER", null);
        using var signingKey = new TempEnvironmentVariable("SQUIRIX_JWT_SIGNING_KEY", "invalid-base64");
        var security = CreateConfiguration(enableJwt ? 3 : 0);
        var services = new ServiceCollection();

        var enabled = services.AddSquirixSecurityServices(security);

        _ = await Assert.That(enabled).IsEqualTo(enableJwt);
    }

    private static SecurityOptions CreateConfiguration(int caseIndex) => caseIndex switch
    {
        0 => new SecurityOptions(),
        1 => new SecurityOptions { JwtAuthority = " ", JwtSigningKey = " ", JwtAudience = Audience },
        2 => new SecurityOptions { JwtAuthority = Authority, JwtAudience = Audience },
        3 => new SecurityOptions { JwtSigningKey = Convert.ToBase64String(new byte[32]), JwtAudience = Audience, JwtIssuer = Issuer },
        4 => new SecurityOptions { JwtAuthority = Authority, JwtSigningKey = Convert.ToBase64String(new byte[32]), JwtAudience = Audience, JwtIssuer = Issuer },
        _ => throw new ArgumentOutOfRangeException(nameof(caseIndex), caseIndex, "Unknown security configuration case."),
    };
}
