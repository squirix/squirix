using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Networking;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>A host built through the public hosting API applies the backpressure options it is given.</summary>
[Immutable]
public sealed class BackpressureHostingTests : IsolatedStorageTestBase
{
    /// <summary>Per-client limits set on the public options reject the offending client while another client is admitted.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerClientLimitsApplyToHost(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(
            static backpressure =>
            {
                backpressure.MaxInFlight = 8;
                backpressure.SlowdownThreshold = 8;
                backpressure.RejectThreshold = 8;
                backpressure.MaxSlowdownDelay = TimeSpan.Zero;
                backpressure.PerClientMaxInFlight = 1;
            },
            cancellationToken);
        var gate = app.Services.GetRequiredService<IBackpressureGate>();

        var (firstDecision, firstLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-a", cancellationToken);
        var (secondDecision, secondLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-a", cancellationToken);
        var (otherDecision, otherLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-b", cancellationToken);
        firstLease.Dispose();
        secondLease.Dispose();
        otherLease.Dispose();

        _ = await Assert.That(firstDecision.IsAccepted).IsTrue();
        _ = await Assert.That(secondDecision.IsAccepted).IsFalse();
        _ = await Assert.That(secondDecision.RejectReason).IsEqualTo("client_concurrency_limit");
        _ = await Assert.That(otherDecision.IsAccepted).IsTrue();
    }

    /// <summary>A per-client rate limit set on the public options rejects the client that spent its burst.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task PerClientRateLimitAppliesToHost(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(
            static backpressure =>
            {
                backpressure.MaxQueue = 0;
                backpressure.MaxSlowdownDelay = TimeSpan.Zero;
                backpressure.PerClientRateLimitPerSecond = 1;
                backpressure.PerClientRateLimitBurst = 1;
            },
            cancellationToken);
        var gate = app.Services.GetRequiredService<IBackpressureGate>();

        var (firstDecision, firstLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-a", cancellationToken);
        firstLease.Dispose();
        var (limitedDecision, limitedLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-a", cancellationToken);
        limitedLease.Dispose();
        var (otherDecision, otherLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-b", cancellationToken);
        otherLease.Dispose();

        _ = await Assert.That(firstDecision.IsAccepted).IsTrue();
        _ = await Assert.That(limitedDecision.RejectReason).IsEqualTo("client_rate_limit");
        _ = await Assert.That(otherDecision.IsAccepted).IsTrue();
    }

    /// <summary>A node rate limit set on the public options rejects requests once its burst is spent, for every client.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task NodeRateLimitAppliesToHost(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(
            static backpressure =>
            {
                backpressure.MaxQueue = 0;
                backpressure.MaxSlowdownDelay = TimeSpan.Zero;
                backpressure.NodeRateLimitPerSecond = 1;
                backpressure.NodeRateLimitBurst = 1;
            },
            cancellationToken);
        var gate = app.Services.GetRequiredService<IBackpressureGate>();

        var (firstDecision, firstLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-a", cancellationToken);
        firstLease.Dispose();
        var (limitedDecision, limitedLease) = await gate.AcquireAsync("grpc", "get", "jwt:client-b", cancellationToken);
        limitedLease.Dispose();

        _ = await Assert.That(firstDecision.IsAccepted).IsTrue();
        _ = await Assert.That(limitedDecision.RejectReason).IsEqualTo("node_rate_limit");
    }

    /// <summary>Without a per-client limit the host shares one client id.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DefaultsShareOneClientId(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(static _ => { }, cancellationToken);

        var resolver = app.Services.GetRequiredService<IBackpressureClientIdResolver>();

        _ = await Assert.That(resolver).IsSameReferenceAs(SharedClientIdResolver.Instance);
    }

    /// <summary>A per-client limit set on the public options switches the host to per-caller client ids.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientLimitResolvesCallerIds(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(static backpressure => backpressure.PerClientMaxInFlight = 4, cancellationToken);

        var resolver = app.Services.GetRequiredService<IBackpressureClientIdResolver>();

        _ = await Assert.That(resolver).IsTypeOf<HttpContextClientIdResolver>();
    }

    /// <summary>A rate-only per-client limit also switches the host to per-caller client ids.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClientRateLimitResolvesCallerIds(CancellationToken cancellationToken)
    {
        await using var app = await BuildAppAsync(
            static backpressure =>
            {
                backpressure.PerClientRateLimitPerSecond = 5;
                backpressure.PerClientRateLimitBurst = 5;
            },
            cancellationToken);

        var resolver = app.Services.GetRequiredService<IBackpressureClientIdResolver>();

        _ = await Assert.That(resolver).IsTypeOf<HttpContextClientIdResolver>();
    }

    /// <summary>Invalid backpressure options fail the public hosting entry point.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidBackpressureFailsHosting(CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });
        var operation = builder.AddSquirixServerAsync(
            static options =>
            {
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort()));
                options.Backpressure.MaxInFlight = 0;
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);

        var ex = await NodeAsyncAssert.ThrowsAsync<ArgumentException>(operation);

        _ = await Assert.That(ex.Message).Contains("MaxInFlight", StringComparison.Ordinal);
    }

    private static async Task<WebApplication> BuildAppAsync(Action<SquirixServerBackpressureOptions> configure, CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { EnvironmentName = "Development" });

        // The frozen clock keeps rate-limit buckets from refilling during the test.
        _ = builder.Services.AddSingleton<TimeProvider>(new FakeTimeProvider());
        _ = await builder.AddSquirixServerAsync(
            options =>
            {
                options.Uri = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("localhost", ListenPortPool.ServerUnitTests.AllocatePort()));
                configure(options.Backpressure);
            },
            loadDiscoveredSettings: false,
            cancellationToken: cancellationToken);
        return builder.Build();
    }
}
