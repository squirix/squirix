using System.Security.Claims;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Http;
using Rocks;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Memory;

/// <summary>Covers JWT / connection / missing-context backpressure client id resolution.</summary>
[Immutable]
public sealed class BackpressureClientIdResolverTests : ServerUnitTestBase
{
    /// <summary>Blank NameIdentifier is ignored so a raw subclaim can still scope the client id.</summary>
    [Test]
    public async Task RawSubUsedWhenNameIdentifierBlank()
    {
        var blankSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = blankSubExpectations.Setups.HttpContext.Gets()
                                .ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "   "), new Claim("sub", "oidc-subject"))));
        _ = blankSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(blankSubExpectations.Instance());

        _ = await Assert.That(resolver.Resolve()).IsEqualTo("jwt:oidc-subject");
    }

    /// <summary>Raw JWT subclaim is used when NameIdentifier is absent.</summary>
    [Test]
    public async Task RawSubUsedWhenNameIdentifierMissing()
    {
        var missingSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = missingSubExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim("sub", "oidc-subject"))));
        _ = missingSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(missingSubExpectations.Instance());

        _ = await Assert.That(resolver.Resolve()).IsEqualTo("jwt:oidc-subject");
    }

    /// <summary>Resolved client ids are cached on the HttpContext for the request lifetime.</summary>
    [Test]
    public async Task ResolveCachesClientIdOnHttpContext()
    {
        var accessorExpectations = new IHttpContextAccessorCreateExpectations();
        _ = accessorExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a"))));
        _ = accessorExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(accessorExpectations.Instance());

        var first = resolver.Resolve();
        var second = resolver.Resolve();

        _ = await Assert.That(first).IsEqualTo("jwt:tenant-a");
        _ = await Assert.That(second).IsSameReferenceAs(first);
    }

    /// <summary>Anonymous requests fall back to the ASP.NET Core connection id.</summary>
    [Test]
    public async Task ResolveUsesConnectionIdWhenAnonymous()
    {
        var conn42Expectations = new IHttpContextAccessorCreateExpectations();
        _ = conn42Expectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-42"));
        _ = conn42Expectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(conn42Expectations.Instance());

        _ = await Assert.That(resolver.Resolve()).IsEqualTo("conn:conn-42");
    }

    /// <summary>Authenticated JWT subject becomes a principal-scoped client id.</summary>
    [Test]
    public async Task ResolveUsesJwtSubjectWhenAuthenticated()
    {
        var jwtExpectations = new IHttpContextAccessorCreateExpectations();
        _ = jwtExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a"))));
        _ = jwtExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(jwtExpectations.Instance());

        _ = await Assert.That(resolver.Resolve()).IsEqualTo("jwt:tenant-a");
    }

    /// <summary>In-process calls without HttpContext share the runtime bucket.</summary>
    [Test]
    public async Task ResolveUsesRuntimeWhenHttpContextMissing()
    {
        var missingExpectations = new IHttpContextAccessorCreateExpectations();
        _ = missingExpectations.Setups.HttpContext.Gets().ReturnValue(null);
        _ = missingExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(missingExpectations.Instance());
        _ = await Assert.That(resolver.Resolve()).IsEqualTo(HttpContextClientIdResolver.MissingHttpContextClientId);
    }

    /// <summary>Authenticated principals without a subject fall back to the connection id.</summary>
    [Test]
    public async Task ResolverUsesConnectionWithoutSubject()
    {
        var noSubExpectations = new IHttpContextAccessorCreateExpectations();
        _ = noSubExpectations.Setups.HttpContext.Gets().ReturnValue(CreateContext("conn-no-sub", AuthenticatedWithoutClaims()));
        _ = noSubExpectations.Setups.HttpContext.Sets(Arg.Any<HttpContext?>());
        var resolver = new HttpContextClientIdResolver(noSubExpectations.Instance());

        _ = await Assert.That(resolver.Resolve()).IsEqualTo("conn:conn-no-sub");
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
