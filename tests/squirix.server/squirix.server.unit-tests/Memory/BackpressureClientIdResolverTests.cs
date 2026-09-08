using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Covers JWT / connection / missing-context backpressure client id resolution.</summary>
[Immutable]
public sealed class BackpressureClientIdResolverTests : ServerUnitTestBase
{
    /// <summary>Resolved client ids are cached on the HttpContext for the request lifetime.</summary>
    [Fact]
    public void ResolveCachesClientIdOnHttpContext()
    {
        var accessorExpectations = new IHttpContextAccessorCreateExpectations();
        _ = accessorExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a"))));
        _ = accessorExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(accessorExpectations.Instance());

        var first = resolver.Resolve();
        var second = resolver.Resolve();

        Assert.Equal("jwt:tenant-a", first);
        Assert.Same(first, second);
    }

    /// <summary>Anonymous requests fall back to the ASP.NET Core connection id.</summary>
    [Fact]
    public void ResolveUsesConnectionIdWhenAnonymous()
    {
        var conn42Expectations = new IHttpContextAccessorCreateExpectations();
        _ = conn42Expectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-42"));
        _ = conn42Expectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(conn42Expectations.Instance());

        Assert.Equal("conn:conn-42", resolver.Resolve());
    }

    /// <summary>Authenticated principals without a subject fall back to the connection id.</summary>
    [Fact]
    public void ResolverUsesConnectionWithoutSubject()
    {
        var noSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = noSubExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-no-sub", AuthenticatedWithoutClaims()));
        _ = noSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(noSubExpectations.Instance());

        Assert.Equal("conn:conn-no-sub", resolver.Resolve());
    }

    /// <summary>Authenticated JWT subject becomes a principal-scoped client id.</summary>
    [Fact]
    public void ResolveUsesJwtSubjectWhenAuthenticated()
    {
        var jwtExpectations = new IHttpContextAccessorCreateExpectations();
        _ = jwtExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a"))));
        _ = jwtExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(jwtExpectations.Instance());

        Assert.Equal("jwt:tenant-a", resolver.Resolve());
    }

    /// <summary>Blank NameIdentifier is ignored so a raw subclaim can still scope the client id.</summary>
    [Fact]
    public void RawSubUsedWhenNameIdentifierBlank()
    {
        var blankSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = blankSubExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "   "), new Claim("sub", "oidc-subject"))));
        _ = blankSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(blankSubExpectations.Instance());

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>Raw JWT subclaim is used when NameIdentifier is absent.</summary>
    [Fact]
    public void RawSubUsedWhenNameIdentifierMissing()
    {
        var missingSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = missingSubExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim("sub", "oidc-subject"))));
        _ = missingSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(missingSubExpectations.Instance());

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>In-process calls without HttpContext share the runtime bucket.</summary>
    [Fact]
    public void ResolveUsesRuntimeWhenHttpContextMissing()
    {
        var missingExpectations = new IHttpContextAccessorCreateExpectations();
        _ = missingExpectations.Setups.HttpContext.Gets().ReturnValue(null);
        _ = missingExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(missingExpectations.Instance());
        Assert.Equal(HttpContextClientIdResolver.MissingHttpContextClientId, resolver.Resolve());
    }

    private static ClaimsPrincipal Authenticated(params Claim[] claims) => new(new ClaimsIdentity(claims, "Bearer"));

    private static ClaimsPrincipal AuthenticatedWithoutClaims() => new(new ClaimsIdentity("Bearer"));

    private static DefaultHttpContext CreateContext(string connectionId, ClaimsPrincipal? user = null) => new()
    {
        Connection =
        {
            Id = connectionId,
        },
        User = user ?? new ClaimsPrincipal(),
    };
}
