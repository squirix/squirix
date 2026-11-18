using System;
using System.IdentityModel.Tokens.Jwt;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using Squirix.Client;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.E2ETests;

/// <summary>Exercises physical transport authentication and renewal through the SDK.</summary>
public sealed class ConnectionAuthenticationTests
{
    /// <summary>Renews a gracefully closed connection while preserving its already admitted operation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CloseRenewsAndDrainsAdmittedRequest(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new ExpiryProbe();
        var providerCalls = 0;
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(CloseRenewsAndDrainsAdmittedRequest),
            TestNodeSecurityOptions.FromJwtCredentials(credentials),
            timeProvider: TimeProvider.System,
            configure: (nodeId, services) =>
            {
                ArgumentException.ThrowIfNullOrEmpty(nodeId);
                _ = services.AddSingleton(probe);
                _ = services.AddSingleton<IAuthorizationHandler>(probe);
                _ = services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, options => options.Events.OnTokenValidated = probe.CaptureConnectionAsync);
            },
            cancellationToken: cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        var options = new SquirixClientOptions
        {
            BearerTokenProvider = token =>
            {
                token.ThrowIfCancellationRequested();
                _ = Interlocked.Increment(ref providerCalls);
                return ValueTask.FromResult(CreateToken(credentials, TimeSpan.FromMinutes(5)));
            },
        };
        options.Endpoints.Add(cluster.GetUri("nodeA"));
        await using var client = await SquirixClient.ConnectAsync(options, handler, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        for (var i = 0; i < 8; i++)
            _ = await cache.GetValueAsync("warm", cancellationToken);
        _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(1);

        probe.Arm();
        var admitted = cache.GetValueAsync("admitted", cancellationToken);
        try
        {
            await probe.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            var lifetime = await probe.Connection.Task.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            lifetime.RequestClose();
            _ = await Assert.That(admitted.IsCompleted).IsFalse();
        }
        finally
        {
            _ = probe.Release.TrySetResult();
        }

        var response = await admitted.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        await cache.WaitUntilValueAsync(
            async (current, token) =>
            {
                var renewed = await current.GetValueAsync("renewed", token);
                _ = await Assert.That(renewed.Found).IsFalse();
                return Volatile.Read(ref providerCalls) >= 2;
            },
            cancellationToken);
        _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(2);
    }

    /// <summary>Maps a real handshake denial to a terminal SDK authentication failure without retries or bootstrap failover.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidCredentialIsNotRetried(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var wrongKey = TestJwtHelper.CreateRandomCredentials(credentials.Issuer, credentials.Audience);
        var invalidToken = TestJwtHelper.CreateBearerToken(wrongKey);
        var providerCalls = 0;
        var secondaryProbe = new SecondaryProbe();
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(InvalidCredentialIsNotRetried),
            TestNodeSecurityOptions.FromJwtCredentials(credentials),
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        await using var secondary = await HostedCluster.StartSingleNodeAsync(
            nameof(InvalidCredentialIsNotRetried) + "Secondary",
            TestNodeSecurityOptions.FromJwtCredentials(wrongKey),
            timeProvider: TimeProvider.System,
            configure: (nodeId, services) =>
            {
                ArgumentException.ThrowIfNullOrEmpty(nodeId);
                _ = services.AddSingleton(secondaryProbe);
                _ = services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, options => options.Events.OnMessageReceived = secondaryProbe.OnMessageReceivedAsync);
            },
            cancellationToken: cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        var options = new SquirixClientOptions
        {
            BearerTokenProvider = token =>
            {
                token.ThrowIfCancellationRequested();
                _ = Interlocked.Increment(ref providerCalls);
                return ValueTask.FromResult(invalidToken);
            },
        };
        options.Endpoints.Add(cluster.GetUri("nodeA"));
        options.Endpoints.Add(secondary.GetUri("nodeA"));
        var exception = await NodeAsyncAssert.ThrowsAsync<SquirixTransportException, ISquirixClient>(
            SquirixClient.ConnectAsync(options, handler, cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(CacheTransportStatusCode.Unauthenticated);
        _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(1);
        _ = await Assert.That(secondaryProbe.Handshakes).IsEqualTo(0);

        using var controlHandler = LoopbackHttp.CreateHandler();
        var controlOptions = new SquirixClientOptions { BearerTokenProvider = _ => ValueTask.FromResult(invalidToken) };
        controlOptions.Endpoints.Add(secondary.GetUri("nodeA"));
        await using var controlClient = await SquirixClient.ConnectAsync(controlOptions, controlHandler, cancellationToken);
        var controlCache = await controlClient.GetCacheAsync<string>("default", cancellationToken);
        _ = await Assert.That((await controlCache.GetValueAsync("denied", cancellationToken)).Found).IsFalse();
        await controlCache.SetAsync("control", "accepted", cancellationToken: cancellationToken);
        _ = await Assert.That((await controlCache.GetValueAsync("control", cancellationToken)).Value).IsEqualTo("accepted");
        _ = await Assert.That(secondaryProbe.Handshakes).IsEqualTo(1);
        _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(1);
    }

    /// <summary>Retains an authenticated primary when a secondary rejects the bootstrap credential.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task DeniedSecondaryKeepsWorkingPrimary(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var wrongKey = TestJwtHelper.CreateRandomCredentials(credentials.Issuer, credentials.Audience);
        await using var primary = await HostedCluster.StartSingleNodeAsync(
            nameof(DeniedSecondaryKeepsWorkingPrimary),
            TestNodeSecurityOptions.FromJwtCredentials(credentials),
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        await using var secondary = await HostedCluster.StartSingleNodeAsync(
            nameof(DeniedSecondaryKeepsWorkingPrimary) + "Secondary",
            TestNodeSecurityOptions.FromJwtCredentials(wrongKey),
            timeProvider: TimeProvider.System,
            cancellationToken: cancellationToken);
        using var handler = LoopbackHttp.CreateHandler();
        var token = TestJwtHelper.CreateBearerToken(credentials);
        var options = new SquirixClientOptions { BearerTokenProvider = _ => ValueTask.FromResult(token) };
        options.Endpoints.Add(primary.GetUri("nodeA"));
        options.Endpoints.Add(secondary.GetUri("nodeA"));

        await using var client = await SquirixClient.ConnectAsync(options, handler, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        await cache.SetAsync("key", "primary-value", cancellationToken: cancellationToken);
        var result = await cache.GetValueAsync("key", cancellationToken);
        _ = await Assert.That(result.Value).IsEqualTo("primary-value");
    }

    private static string CreateToken(TestJwtCredentials credentials, TimeSpan lifetime)
    {
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(credentials.Issuer, credentials.Audience, notBefore: now.AddMinutes(-1), expires: now.Add(lifetime), signingCredentials: new SigningCredentials(new SymmetricSecurityKey(credentials.GetSigningKey()), SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private sealed class SecondaryProbe
    {
        private int _handshakes;

        internal int Handshakes => Volatile.Read(ref _handshakes);

        internal Task OnMessageReceivedAsync(MessageReceivedContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            _ = Interlocked.Increment(ref _handshakes);
            return Task.CompletedTask;
        }
    }

    private sealed class ExpiryProbe : IAuthorizationHandler
    {
        private int _armed;

        internal TaskCompletionSource<IConnectionLifetimeNotificationFeature> Connection { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task HandleAsync(AuthorizationHandlerContext context)
        {
            if (context.Resource is not HttpContext request || Interlocked.Exchange(ref _armed, 0) == 0)
                return;

            _ = Entered.TrySetResult();
            await Release.Task.WaitAsync(request.RequestAborted);
        }

        internal Task CaptureConnectionAsync(TokenValidatedContext context)
        {
            ArgumentNullException.ThrowIfNull(context);
            var lifetime = context.HttpContext.Features.Get<IConnectionLifetimeNotificationFeature>();
            if (lifetime != null)
                _ = Connection.TrySetResult(lifetime);

            return Task.CompletedTask;
        }

        internal void Arm() => Volatile.Write(ref _armed, 1);
    }
}
