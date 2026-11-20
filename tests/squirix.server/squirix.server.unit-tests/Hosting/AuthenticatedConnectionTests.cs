using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies connection admission bounds and graceful expiry notification.</summary>
[Immutable]
public sealed class AuthenticatedConnectionTests
{
    /// <summary>Checks the lower bound again when a clock moves backwards and excludes the expiry instant.</summary>
    [Test]
    public async Task LifetimeChecksBothBounds()
    {
        var lower = DateTime.UnixEpoch.AddMinutes(1);
        var upper = lower.AddMinutes(1);
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(), (lower, upper), new JwtValidationOptions(), null, null, static () => { })!;
        _ = await Assert.That(connection.IsWithinLifetime(lower.AddTicks(-1))).IsFalse();
        _ = await Assert.That(connection.IsWithinLifetime(lower)).IsTrue();
        _ = await Assert.That(connection.IsWithinLifetime(upper.AddTicks(-1))).IsTrue();
        _ = await Assert.That(connection.IsWithinLifetime(upper)).IsFalse();
    }

    /// <summary>Expiry asks the binary session to stop admitting work without aborting the connection.</summary>
    [Test]
    public async Task ExpiryRequestsGracefulClose()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var lifetime = new Lifetime();
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(), (DateTime.UnixEpoch, DateTime.UnixEpoch.AddMinutes(1)), new JwtValidationOptions(), null, null, lifetime.RequestClose)!;
        await using var timer = connection.CreateExpiryTimer(clock);
        clock.Advance(TimeSpan.FromSeconds(59));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(0);
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(1);
        clock.Advance(TimeSpan.FromMinutes(1));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(1);
    }

    /// <summary>The connection owns identity claims and property dictionaries independently of the validated principal.</summary>
    [Test]
    public async Task SnapshotOwnsIdentityClaims()
    {
        var identity = CreateIdentity();
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(identity), (DateTime.UnixEpoch, DateTime.MaxValue), new JwtValidationOptions(), null, null, static () => { })!;
        identity.FindFirst("seed")!.Properties["state"] = "changed";
        identity.AddClaim(new Claim("source-change", "unexpected"));
        var snapshot = connection.ClonePrincipal()!;
        _ = await Assert.That(snapshot.HasClaim("seed", "original")).IsTrue();
        _ = await Assert.That(snapshot.FindFirst("seed")!.Properties["state"]).IsEqualTo("original");
        _ = await Assert.That(snapshot.HasClaim("source-change", "unexpected")).IsFalse();
    }

    /// <summary>Changing an existing request identity cannot mutate another request or the connection snapshot.</summary>
    [Test]
    public async Task RequestIdentityCopiesAreIndependent()
    {
        var identity = CreateIdentity();
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(identity), (DateTime.UnixEpoch, DateTime.MaxValue), new JwtValidationOptions(), null, null, static () => { })!;
        var first = connection.ClonePrincipal()!;
        var second = connection.ClonePrincipal()!;
        first.FindFirst("seed")!.Properties["state"] = "changed";
        if (first.Identity is ClaimsIdentity firstIdentity)
        {
            firstIdentity.RemoveClaim(firstIdentity.FindFirst("seed"));
            firstIdentity.AddClaim(new Claim("request-change", "unexpected"));
        }

        var later = connection.ClonePrincipal()!;
        _ = await Assert.That(second.HasClaim("seed", "original")).IsTrue();
        _ = await Assert.That(second.FindFirst("seed")!.Properties["state"]).IsEqualTo("original");
        _ = await Assert.That(second.HasClaim("request-change", "unexpected")).IsFalse();
        _ = await Assert.That(later.HasClaim("seed", "original")).IsTrue();
        _ = await Assert.That(later.FindFirst("seed")!.Properties["state"]).IsEqualTo("original");
        _ = await Assert.That(later.HasClaim("request-change", "unexpected")).IsFalse();
    }

    private static ClaimsIdentity CreateIdentity()
    {
        var identity = new ClaimsIdentity([new Claim("seed", "original")], "Bearer");
        identity.FindFirst("seed")!.Properties.Add("state", "original");
        return identity;
    }

    private sealed class Lifetime
    {
        internal int Requests { get; private set; }

        internal void RequestClose() => Requests++;
    }
}
