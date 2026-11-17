using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Connections.Features;
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
        var connection = new AuthenticatedConnection(new ClaimsPrincipal(), (lower, upper), new JwtBearerOptions(), null, null, new Lifetime());
        _ = await Assert.That(connection.IsWithinLifetime(lower.AddTicks(-1))).IsFalse();
        _ = await Assert.That(connection.IsWithinLifetime(lower)).IsTrue();
        _ = await Assert.That(connection.IsWithinLifetime(upper.AddTicks(-1))).IsTrue();
        _ = await Assert.That(connection.IsWithinLifetime(upper)).IsFalse();
    }

    /// <summary>Expiry asks HTTP/2 to stop admitting work without aborting the connection.</summary>
    [Test]
    public async Task ExpiryRequestsGracefulClose()
    {
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var lifetime = new Lifetime();
        var connection = new AuthenticatedConnection(new ClaimsPrincipal(), (DateTime.UnixEpoch, DateTime.UnixEpoch.AddMinutes(1)), new JwtBearerOptions(), null, null, lifetime);
        await using var timer = connection.CreateExpiryTimer(clock);
        clock.Advance(TimeSpan.FromSeconds(59));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(0);
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(1);
        clock.Advance(TimeSpan.FromMinutes(1));
        _ = await Assert.That(lifetime.Requests).IsEqualTo(1);
    }

    private sealed class Lifetime : IConnectionLifetimeNotificationFeature
    {
        public CancellationToken ConnectionClosedRequested { get; set; }

        internal int Requests { get; private set; }

        public void RequestClose() => Requests++;
    }
}
