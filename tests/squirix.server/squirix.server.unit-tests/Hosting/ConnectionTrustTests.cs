using System;
using System.IdentityModel.Tokens.Jwt;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Microsoft.IdentityModel.Tokens;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies conservative OIDC trust history without token revalidation on requests.</summary>
[Immutable]
public sealed class ConnectionTrustTests
{
    /// <summary>Unknown internal LKG entries never receive an inferred token-length trust window.</summary>
    [Test]
    public async Task UnobservedHistoricalKeyIsRejected()
    {
        var options = CreateOptions(out _);
        var key = new SymmetricSecurityKey(new byte[32]);
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = key };
        var expiry = DateTime.MaxValue;
        var accepted = new ConnectionTrustHistory().TryAuthorize(options, new OpenIdConnectConfiguration(), token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsFalse();
    }

    /// <summary>A recorded current validation caps historical trust even if the manager lifetime later grows.</summary>
    /// <param name="issuerMode">Selects metadata, ValidIssuer, or ValidIssuers trust.</param>
    [Test]
    [Arguments(0)]
    [Arguments(1)]
    [Arguments(2)]
    public async Task HistoricalLifetimeCannotBeExtended(int issuerMode)
    {
        var options = CreateOptions(out var manager);
        ConfigureIssuer(options.TokenValidationParameters, issuerMode);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = issuerMode == 0 ? "issuer" : "metadata" };
        configuration.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = key };
        var started = DateTime.UnixEpoch;
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var current = history.TryAuthorize(options, configuration, token, started, TimeSpan.MaxValue, ref expiry, out _);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        expiry = DateTime.MaxValue;
        var historical = history.TryAuthorize(options, new OpenIdConnectConfiguration(), token, started.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var recorded);
        _ = await Assert.That(current).IsTrue();
        _ = await Assert.That(historical).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(started.AddMinutes(1));
        _ = await Assert.That(recorded).IsEqualTo(started);
        expiry = DateTime.MaxValue;
        _ = await Assert.That(history.TryAuthorize(options, new OpenIdConnectConfiguration(), token, started.AddMinutes(1), TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>Reused keys retain historical issuer bounds unless the issuer is explicitly trusted as current.</summary>
    /// <param name="explicitIssuer">Whether the original issuer remains explicitly trusted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ReusedKeyUsesBoundedIssuerTrust(bool explicitIssuer)
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        if (explicitIssuer)
            options.TokenValidationParameters.ValidIssuer = "previous";
        var key = new SymmetricSecurityKey(new byte[32]);
        var original = new OpenIdConnectConfiguration { Issuer = "previous" };
        original.SigningKeys.Add(key);
        var replacement = new OpenIdConnectConfiguration { Issuer = "replacement" };
        replacement.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "previous") { SigningKey = key };
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var observed = history.TryAuthorize(options, original, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        var accepted = history.TryAuthorize(options, replacement, token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var historical);
        _ = await Assert.That(observed).IsTrue();
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(historical).IsEqualTo(explicitIssuer ? null : DateTime.UnixEpoch);
        _ = await Assert.That(expiry).IsEqualTo(explicitIssuer ? DateTime.MaxValue : DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>Explicit issuer trust combines with exact metadata keys without trusting a matching key identifier.</summary>
    /// <param name="issuerMode">Selects absent, ValidIssuer, or ValidIssuers trust.</param>
    /// <param name="sameKey">Whether the token used the exact configured key object.</param>
    /// <param name="expected">Whether current trust can be proven.</param>
    [Test]
    [Arguments(1, true, true)]
    [Arguments(2, true, true)]
    [Arguments(0, true, false)]
    [Arguments(1, false, false)]
    public async Task CurrentKeyUsesConfiguredIssuer(int issuerMode, bool sameKey, bool expected)
    {
        var options = CreateOptions(out _);
        ConfigureIssuer(options.TokenValidationParameters, issuerMode);
        var key = new SymmetricSecurityKey(new byte[32]) { KeyId = "same" };
        var configuration = new OpenIdConnectConfiguration { Issuer = "metadata" };
        configuration.SigningKeys.Add(key);
        var tokenKey = sameKey ? key : new SymmetricSecurityKey(new byte[32]) { KeyId = "same" };
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = tokenKey };
        var expiry = DateTime.MaxValue;
        var accepted = new ConnectionTrustHistory().TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out var historical);
        _ = await Assert.That(accepted).IsEqualTo(expected);
        _ = await Assert.That(historical).IsNull();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.MaxValue);
    }

    /// <summary>Disabling LKG immediately prevents historical bindings.</summary>
    [Test]
    public async Task DisabledHistoricalTrustIsRejected()
    {
        var options = CreateOptions(out _);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = key };
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        options.ConfigurationManager = new ConfigurationManager<OpenIdConnectConfiguration>("https://issuer/.well-known/openid-configuration", new OpenIdConnectConfigurationRetriever()) { UseLastKnownGoodConfiguration = false };
        _ = await Assert.That(history.TryAuthorize(options, new OpenIdConnectConfiguration(), token, DateTime.UnixEpoch.AddSeconds(1), TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>A reduced LKG lifetime immediately narrows already observed trust.</summary>
    [Test]
    public async Task HistoricalLifetimeShrinks()
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = key };
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        var accepted = history.TryAuthorize(options, new OpenIdConnectConfiguration(), token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>Equivalent key material never borrows the validity proof of another configuration object.</summary>
    [Test]
    public async Task DistinctKeyCannotBorrowCurrentTrust()
    {
        var options = CreateOptions(out _);
        var configuration = new OpenIdConnectConfiguration { Issuer = "issuer" };
        configuration.SigningKeys.Add(new SymmetricSecurityKey(new byte[32]));
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = new SymmetricSecurityKey(new byte[32]) };
        var expiry = DateTime.MaxValue;
        _ = await Assert.That(new ConnectionTrustHistory().TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
    }

    /// <summary>A lifetime increase during validation cannot exceed the pre-validation trust budget.</summary>
    [Test]
    public async Task ValidationLifetimeIncreaseIsBounded()
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromHours(1);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = "issuer" };
        configuration.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "issuer") { SigningKey = key };
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        _ = history.TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.FromMinutes(1), ref expiry, out _);
        var accepted = history.TryAuthorize(options, new OpenIdConnectConfiguration(), token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.FromHours(1), ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.UnixEpoch.AddMinutes(1));
    }

    /// <summary>An explicit static key can use the current metadata issuer without borrowing historical issuer trust.</summary>
    /// <param name="explicitIssuer">Whether the original issuer also has explicit options trust.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task StaticKeyTracksMetadataIssuer(bool explicitIssuer)
    {
        var options = CreateOptions(out _);
        var key = new SymmetricSecurityKey(new byte[32]);
        options.TokenValidationParameters.IssuerSigningKey = key;
        if (explicitIssuer)
            options.TokenValidationParameters.ValidIssuer = "metadata";
        var original = new OpenIdConnectConfiguration { Issuer = "metadata" };
        original.SigningKeys.Add(new SymmetricSecurityKey(new byte[32]));
        var replacement = new OpenIdConnectConfiguration { Issuer = "replacement" };
        var token = new JwtSecurityToken(issuer: "metadata") { SigningKey = key };
        var history = new ConnectionTrustHistory();
        var expiry = DateTime.MaxValue;
        var accepted = history.TryAuthorize(options, original, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out var historical);
        var changed = history.TryAuthorize(options, replacement, token, DateTime.UnixEpoch.AddSeconds(30), TimeSpan.MaxValue, ref expiry, out var changedHistory);
        var unknown = new JwtSecurityToken(issuer: "untrusted") { SigningKey = key };
        var unknownAccepted = history.TryAuthorize(options, original, unknown, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _);
        _ = await Assert.That(accepted).IsTrue();
        _ = await Assert.That(historical).IsNull();
        _ = await Assert.That(changed).IsEqualTo(explicitIssuer);
        _ = await Assert.That(changedHistory).IsNull();
        _ = await Assert.That(unknownAccepted).IsFalse();
        _ = await Assert.That(expiry).IsEqualTo(DateTime.MaxValue);
    }

    private static void ConfigureIssuer(TokenValidationParameters parameters, int issuerMode)
    {
        if (issuerMode == 1)
            parameters.ValidIssuer = "issuer";
        if (issuerMode == 2)
            parameters.ValidIssuers = ["issuer"];
    }

    private static JwtBearerOptions CreateOptions(out ConfigurationManager<OpenIdConnectConfiguration> manager)
    {
        manager = new ConfigurationManager<OpenIdConnectConfiguration>("https://issuer/.well-known/openid-configuration", new OpenIdConnectConfigurationRetriever());
        return new JwtBearerOptions { ConfigurationManager = manager };
    }
}
