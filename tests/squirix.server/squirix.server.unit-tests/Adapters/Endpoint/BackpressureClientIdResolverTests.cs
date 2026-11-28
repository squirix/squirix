using System.Diagnostics.Metrics;
using System.Security.Claims;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Composition;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.Node.Observability;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Adapters.Endpoint;

/// <summary>Covers JWT / connection / missing-context backpressure client id resolution.</summary>
[Immutable]
public sealed class BackpressureClientIdResolverTests : ServerUnitTestBase
{
    private static readonly NodeProvider Services = new TestServiceCollection().BuildServiceProvider();

    /// <summary>Blank NameIdentifier is ignored so a raw subclaim can still scope the client id.</summary>
    [Test]
    public async Task RawSubUsedWhenNameIdentifierBlank()
    {
        var context = CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "   "), new Claim("sub", "oidc-subject")));

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo("jwt:oidc-subject");
    }

    /// <summary>Raw JWT subclaim is used when NameIdentifier is absent.</summary>
    [Test]
    public async Task RawSubUsedWhenNameIdentifierMissing()
    {
        var context = CreateContext("conn-1", Authenticated(new Claim("sub", "oidc-subject")));

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo("jwt:oidc-subject");
    }

    /// <summary>Resolved client ids are cached on the RequestContext for the request lifetime.</summary>
    [Test]
    public async Task ResolveCachesClientIdOnRequestContext()
    {
        var context = CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, "tenant-a")));

        var first = BinaryClientIdResolver.ResolveRequest(context);
        var second = BinaryClientIdResolver.ResolveRequest(context);

        _ = await Assert.That(first).IsEqualTo("jwt:tenant-a");
        _ = await Assert.That(second).IsSameReferenceAs(first);
    }

    /// <summary>Anonymous requests fall back to the native connection id.</summary>
    /// <param name="connectionId">The connection identifier.</param>
    /// <param name="expected">The exact prefixed identifier.</param>
    [Test]
    [Arguments("conn-42", "conn:conn-42")]
    [Arguments("соединение:東京", "conn:соединение:東京")]
    public async Task ResolveUsesConnectionIdWhenAnonymous(string connectionId, string expected)
    {
        var context = CreateContext(connectionId);

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo(expected);
    }

    /// <summary>Authenticated JWT subject becomes a principal-scoped client id.</summary>
    /// <param name="subject">The authenticated subject.</param>
    /// <param name="expected">The exact prefixed identifier.</param>
    [Test]
    [Arguments("tenant-a", "jwt:tenant-a")]
    [Arguments("арендатор:東京", "jwt:арендатор:東京")]
    public async Task ResolveUsesJwtSubjectWhenAuthenticated(string subject, string expected)
    {
        var context = CreateContext("conn-1", Authenticated(new Claim(ClaimTypes.NameIdentifier, subject)));

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo(expected);
    }

    /// <summary>In-process calls without RequestContext share the runtime bucket.</summary>
    [Test]
    public async Task MissingRequestUsesRuntimeId()
    {
        var resolver = new BinaryClientIdResolver();
        _ = await Assert.That(resolver.Resolve()).IsEqualTo(ClientIds.Runtime);
    }

    /// <summary>Authenticated principals without a subject fall back to the connection id.</summary>
    [Test]
    public async Task ResolverUsesConnectionWithoutSubject()
    {
        var context = CreateContext("conn-no-sub", AuthenticatedWithoutClaims());

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo("conn:conn-no-sub");
    }

    /// <summary>An owner-routed call from another node resolves to the internal id, not to its forwarding connection.</summary>
    [Test]
    public async Task ResolverMarksInternalOwnerCalls()
    {
        var context = CreateContext("forwarder-conn");
        context.Forwarded = true;
        using var meter = new Meter("Squirix");
        using var gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(meter), System.TimeProvider.System);
        var admission = new CacheRequestAdmission(gate, new BinaryClientIdResolver());

        _ = await Assert.That(admission.ResolveClientId(context)).IsEqualTo(ClientIds.InternalOwner);
    }

    /// <summary>Verifies the shared-mode resolver returns the internal id only inside an internal owner invocation.</summary>
    [Test]
    public async Task SharedResolverMarksInternalOwnerCalls()
    {
        using var meter = new Meter("Squirix");
        using var gate = new AdmissionGate(new AdmissionOptions(), new BackpressureMetrics(meter), System.TimeProvider.System);
        var admission = new CacheRequestAdmission(gate, SharedClientIdResolver.Instance);
        var caller = CreateContext("caller");
        var owner = CreateContext("owner");
        owner.Forwarded = true;

        _ = await Assert.That(admission.ResolveClientId(caller)).IsEqualTo(ClientIds.Runtime);
        _ = await Assert.That(admission.ResolveClientId(owner)).IsEqualTo(ClientIds.InternalOwner);
    }

    /// <summary>A JWT subject matching the reserved owner id remains an external client.</summary>
    [Test]
    public async Task ResolverPrefixesInternalSubject()
    {
        var context = CreateContext("conn-1", Authenticated(new Claim("sub", "internal")));

        _ = await Assert.That(BinaryClientIdResolver.ResolveRequest(context)).IsEqualTo("jwt:internal");
    }

    private static ClaimsPrincipal Authenticated(params Claim[] claims) => new(new ClaimsIdentity(claims, "Bearer"));

    private static ClaimsPrincipal AuthenticatedWithoutClaims() => new(new ClaimsIdentity("Bearer"));

    private static BinaryRequestContext CreateContext(string connectionId, ClaimsPrincipal? user = null) => new()
    {
        ConnectionId = connectionId,
        RequestServices = Services,
        Path = "/cache",
        User = user ?? new ClaimsPrincipal(),
    };
}
