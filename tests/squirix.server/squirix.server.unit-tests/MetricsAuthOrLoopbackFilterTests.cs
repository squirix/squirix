using System;
using System.Net;
using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Cookies;
using Microsoft.AspNetCore.DataProtection;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Hosting;
using Squirix.Server.Node.Observability.Metrics;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for metrics endpoint loopback-or-authenticated access control.</summary>
[Immutable]
public sealed class MetricsAuthOrLoopbackFilterTests
{
    /// <summary>Verifies loopback clients can scrape metrics without authentication.</summary>
    /// <param name="address">The loopback address.</param>
    [Test]
    [Arguments("127.0.0.1")]
    [Arguments("::1")]
    [Arguments("::ffff:127.0.0.1")]
    public async Task LoopbackAllowedWithoutAuthentication(string address) =>
        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(CreateContext(IPAddress.Parse(address)))).IsTrue();

    /// <summary>Verifies remote authenticated clients can scrape metrics.</summary>
    [Test]
    public async Task RemoteAllowedWhenAuthenticated()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var services = CreateServices(credentials);
        var context = CreateContext(IPAddress.Parse("203.0.113.10"), services);
        context.Request.Headers.Authorization = $"Bearer {TestJwtHelper.CreateBearerToken(credentials)}";

        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(context)).IsTrue();
        var defaultScheme = await services.GetRequiredService<IAuthenticationSchemeProvider>().GetDefaultAuthenticateSchemeAsync();
        _ = await Assert.That(defaultScheme?.Name).IsEqualTo(CookieAuthenticationDefaults.AuthenticationScheme);
    }

    /// <summary>Verifies remote unauthenticated clients are rejected.</summary>
    /// <param name="token">The invalid or absent bearer token.</param>
    [Test]
    [Arguments("")]
    [Arguments("invalid.jwt.token")]
    public async Task UnauthenticatedRemoteRejected(string token)
    {
        await using var services = CreateServices(TestJwtHelper.CreateRandomCredentials());
        var context = CreateContext(IPAddress.Parse("203.0.113.10"), services);
        context.Request.Headers.Authorization = $"Bearer {token}";
        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(context)).IsFalse();
    }

    /// <summary>A real host cookie does not authorize a remote diagnostic request.</summary>
    [Test]
    public async Task HostCookieDoesNotAuthorizeRemoteRequest()
    {
        await using var services = CreateServices(TestJwtHelper.CreateRandomCredentials());
        await using var signInScope = services.CreateAsyncScope();
        var signIn = CreateContext(IPAddress.Parse("203.0.113.10"), signInScope.ServiceProvider);
        await signIn.SignInAsync(
            CookieAuthenticationDefaults.AuthenticationScheme,
            new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "host-user")], CookieAuthenticationDefaults.AuthenticationScheme)));
        var cookie = signIn.Response.Headers.SetCookie.ToString().Split(';', 2)[0];

        await using var requestScope = services.CreateAsyncScope();
        var context = CreateContext(IPAddress.Parse("203.0.113.10"), requestScope.ServiceProvider);
        context.Request.Headers.Cookie = cookie;
        var hostAuthentication = await context.AuthenticateAsync();
        _ = await Assert.That(hostAuthentication.Succeeded).IsTrue();
        context.User = hostAuthentication.Principal!;

        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(context)).IsFalse();
    }

    /// <summary>Existing identities do not replace an unregistered bearer handler.</summary>
    [Test]
    public async Task RemoteRejectedWithoutBearerRegistration()
    {
        await using var services = CreateServices(null);
        var context = CreateContext(IPAddress.Parse("203.0.113.10"), services);
        context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.Name, "scraper")], "Bearer"));
        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(context)).IsFalse();
    }

    /// <summary>A missing connection address is not treated as loopback.</summary>
    [Test]
    public async Task MissingAddressRequiresBearer()
    {
        await using var services = CreateServices(null);
        _ = await Assert.That(await ConnectionSecurity.IsRequestAuthorizedAsync(CreateContext(null, services))).IsFalse();
    }

    private static DefaultHttpContext CreateContext(IPAddress? remoteIp, IServiceProvider? services = null)
    {
        var http = new DefaultHttpContext
        {
            Connection =
            {
                RemoteIpAddress = remoteIp,
            },
        };

        if (services != null)
            http.RequestServices = services;

        return http;
    }

    private static ServiceProvider CreateServices(TestJwtCredentials? credentials)
    {
        var services = new ServiceCollection();
        _ = services.AddLogging();
        _ = services.AddDataProtection().UseEphemeralDataProtectionProvider();
        _ = services.AddAuthentication(CookieAuthenticationDefaults.AuthenticationScheme).AddCookie();
        if (credentials != null)
        {
            _ = services.AddSquirixSecurityServices(new SecurityOptions
            {
                JwtAudience = credentials.Audience,
                JwtIssuer = credentials.Issuer,
                JwtSigningKey = credentials.Base64SigningKey,
            });
        }

        return services.BuildServiceProvider();
    }
}
