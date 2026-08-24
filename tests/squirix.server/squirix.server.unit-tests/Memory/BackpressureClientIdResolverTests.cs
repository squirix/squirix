using System.Security.Claims;
using Microsoft.AspNetCore.Http;
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
        var accessor = new FixedHttpContextAccessor(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a"))));
        var resolver = new HttpContextClientIdResolver(accessor);

        var first = resolver.Resolve();
        var second = resolver.Resolve();

        Assert.Equal("jwt:tenant-a", first);
        Assert.Same(first, second);
    }

    /// <summary>Anonymous requests fall back to the ASP.NET Core connection id.</summary>
    [Fact]
    public void ResolveUsesConnectionIdWhenAnonymous()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(CreateContext("conn-42")));

        Assert.Equal("conn:conn-42", resolver.Resolve());
    }

    /// <summary>Authenticated principals without a subject fall back to the connection id.</summary>
    [Fact]
    public void ResolverUsesConnectionWithoutSubject()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(CreateContext("conn-no-sub", AuthenticatedWithoutClaims())));

        Assert.Equal("conn:conn-no-sub", resolver.Resolve());
    }

    /// <summary>Authenticated JWT subject becomes a principal-scoped client id.</summary>
    [Fact]
    public void ResolveUsesJwtSubjectWhenAuthenticated()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a")))));

        Assert.Equal("jwt:tenant-a", resolver.Resolve());
    }

    /// <summary>Blank NameIdentifier is ignored so a raw subclaim can still scope the client id.</summary>
    [Fact]
    public void RawSubUsedWhenNameIdentifierBlank()
    {
        var resolver = new HttpContextClientIdResolver(
            new FixedHttpContextAccessor(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "   "), new Claim("sub", "oidc-subject")))));

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>Raw JWT subclaim is used when NameIdentifier is absent.</summary>
    [Fact]
    public void RawSubUsedWhenNameIdentifierMissing()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(CreateContext("conn-1", Authenticated(new Claim("sub", "oidc-subject")))));

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>In-process calls without HttpContext share the runtime bucket.</summary>
    [Fact]
    public void ResolveUsesRuntimeWhenHttpContextMissing()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(null));
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

    private sealed class FixedHttpContextAccessor : IHttpContextAccessor
    {
        internal FixedHttpContextAccessor(HttpContext? context)
        {
            HttpContext = context;
        }

        public HttpContext? HttpContext { get; set; }
    }
}
