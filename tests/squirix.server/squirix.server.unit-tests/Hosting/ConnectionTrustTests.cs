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
    [Test]
    public async Task HistoricalLifetimeCannotBeExtended()
    {
        var options = CreateOptions(out var manager);
        manager.LastKnownGoodLifetime = TimeSpan.FromMinutes(1);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = "issuer" };
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

    /// <summary>The same signing key cannot turn an old issuer into current trust.</summary>
    [Test]
    public async Task ReusedKeyDoesNotHideIssuerChange()
    {
        var options = CreateOptions(out _);
        var key = new SymmetricSecurityKey(new byte[32]);
        var configuration = new OpenIdConnectConfiguration { Issuer = "replacement" };
        configuration.SigningKeys.Add(key);
        var token = new JwtSecurityToken(issuer: "previous") { SigningKey = key };
        var expiry = DateTime.MaxValue;
        _ = await Assert.That(new ConnectionTrustHistory().TryAuthorize(options, configuration, token, DateTime.UnixEpoch, TimeSpan.MaxValue, ref expiry, out _)).IsFalse();
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

    private static JwtBearerOptions CreateOptions(out ConfigurationManager<OpenIdConnectConfiguration> manager)
    {
        manager = new ConfigurationManager<OpenIdConnectConfiguration>("https://issuer/.well-known/openid-configuration", new OpenIdConnectConfigurationRetriever());
        return new JwtBearerOptions { ConfigurationManager = manager };
    }
}
