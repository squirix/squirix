using System.Security.Claims;
using Microsoft.AspNetCore.Http;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.UnitTests.Support;
using Xunit;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Covers JWT / connection / missing-context backpressure client id resolution.</summary>
public sealed class BackpressureClientIdResolverTests : ServerUnitTestBase
{
    /// <summary>Resolved client ids are cached on the HttpContext for the request lifetime.</summary>
    [Fact]
    public void ResolveCachesClientIdOnHttpContext()
    {
        var accessor = new FixedHttpContextAccessor(CreateContext(true, "tenant-a", "conn-1"));
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
        var accessor = new FixedHttpContextAccessor(CreateContext(false, null, "conn-42"));
        var resolver = new HttpContextClientIdResolver(accessor);

        Assert.Equal("conn:conn-42", resolver.Resolve());
    }

    /// <summary>Authenticated principals without a subject fall back to the connection id.</summary>
    [Fact]
    public void ResolveUsesConnectionWhenAuthHasNoSubject()
    {
        var context = new DefaultHttpContext
        {
            Connection =
            {
                Id = "conn-no-sub",
            },
            User = new ClaimsPrincipal(new ClaimsIdentity("Bearer")),
        };
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(context));

        Assert.Equal("conn:conn-no-sub", resolver.Resolve());
    }

    /// <summary>Authenticated JWT subject becomes a principal-scoped client id.</summary>
    [Fact]
    public void ResolveUsesJwtSubjectWhenAuthenticated()
    {
        var accessor = new FixedHttpContextAccessor(CreateContext(true, "tenant-a", "conn-1"));
        var resolver = new HttpContextClientIdResolver(accessor);

        Assert.Equal("jwt:tenant-a", resolver.Resolve());
    }

    /// <summary>Blank NameIdentifier is ignored so a raw sub claim can still scope the client id.</summary>
    [Fact]
    public void ResolveUsesRawSubWhenNameIdentifierIsWhitespace()
    {
        var claimsIdentity = new ClaimsIdentity(
            [
                new Claim(ClaimTypes.NameIdentifier, "   "),
                new Claim("sub", "oidc-subject"),
            ],
            "Bearer");
        var claimsPrincipal = new ClaimsPrincipal(claimsIdentity);
        var context = new DefaultHttpContext
        {
            Connection =
            {
                Id = "conn-1",
            },
            User = claimsPrincipal,
        };
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(context));

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>Raw JWT sub claim is used when NameIdentifier is absent.</summary>
    [Fact]
    public void ResolveUsesRawSubWhenNameIdentifierMissing()
    {
        var context = new DefaultHttpContext
        {
            Connection =
            {
                Id = "conn-1",
            },
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("sub", "oidc-subject")], "Bearer")),
        };
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(context));

        Assert.Equal("jwt:oidc-subject", resolver.Resolve());
    }

    /// <summary>In-process calls without HttpContext share the runtime bucket.</summary>
    [Fact]
    public void ResolveUsesRuntimeWhenHttpContextMissing()
    {
        var resolver = new HttpContextClientIdResolver(new FixedHttpContextAccessor(null));
        Assert.Equal(HttpContextClientIdResolver.MissingHttpContextClientId, resolver.Resolve());
    }

    private static DefaultHttpContext CreateContext(bool authenticated, string? subject, string connectionId)
    {
        var context = new DefaultHttpContext
        {
            Connection =
            {
                Id = connectionId,
            },
        };
        if (!authenticated || subject is null)
            return context;

        context.User = new ClaimsPrincipal(new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, subject)], "Bearer"));
        return context;
    }

    private sealed class FixedHttpContextAccessor : IHttpContextAccessor
    {
        internal FixedHttpContextAccessor(HttpContext? context)
        {
            HttpContext = context;
        }

        public HttpContext? HttpContext { get; set; }
    }
}
