using System;
using System.IdentityModel.Tokens.Jwt;
using System.Threading;
using System.Threading.Tasks;
using Grpc.AspNetCore.Server;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Connections.Features;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.IdentityModel.Tokens;
using Squirix.Client;
using Squirix.E2ETests.Fixtures;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
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
                _ = services.Configure<GrpcServiceOptions>(static options => options.Interceptors.Add<ExpiryProbe>());
                _ = services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, static options => options.TokenValidationParameters.ClockSkew = TimeSpan.Zero);
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
            var lifetime = await probe.Entered.Task.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            lifetime.RequestClose();
            await probe.CloseRequested.Task.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
            _ = await Assert.That(admitted.IsCompleted).IsFalse();
            using var renewalTimeout = new CancellationTokenSource(TimeSpan.FromSeconds(15), TimeProvider.System);
            using var renewalWindow = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, renewalTimeout.Token);
            do
                _ = await cache.GetValueAsync("renewed", renewalWindow.Token);
            while (Volatile.Read(ref providerCalls) == 1);
            _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(2);
            _ = await Assert.That(admitted.IsCompleted).IsFalse();
        }
        finally
        {
            _ = probe.Release.TrySetResult();
        }

        var response = await admitted.WaitAsync(TimeSpan.FromSeconds(15), TimeProvider.System, cancellationToken);
        _ = await Assert.That(response.Found).IsFalse();
        _ = await Assert.That(probe.AdmittedCalls).IsEqualTo(1);
    }

    /// <summary>Maps a real handshake denial to a terminal SDK authentication failure without retries.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task InvalidCredentialIsNotRetried(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var wrongKey = TestJwtHelper.CreateRandomCredentials(credentials.Issuer, credentials.Audience);
        var invalidToken = TestJwtHelper.CreateBearerToken(wrongKey);
        var providerCalls = 0;
        await using var cluster = await HostedCluster.StartSingleNodeAsync(
            nameof(InvalidCredentialIsNotRetried),
            TestNodeSecurityOptions.FromJwtCredentials(credentials),
            timeProvider: TimeProvider.System,
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
        await using var client = await SquirixClient.ConnectAsync(options, handler, cancellationToken);
        var cache = await client.GetCacheAsync<string>("default", cancellationToken);
        var exception = await NodeAsyncAssert.ThrowsAsync<RpcException>(
            cache.SetAsync("denied", "must-not-write", cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(StatusCode.Unauthenticated);
        _ = await Assert.That(Volatile.Read(ref providerCalls)).IsEqualTo(1);
    }

    private static string CreateToken(TestJwtCredentials credentials, TimeSpan lifetime)
    {
        var now = DateTime.UtcNow;
        var token = new JwtSecurityToken(credentials.Issuer, credentials.Audience, notBefore: now.AddMinutes(-1), expires: now.Add(lifetime), signingCredentials: new SigningCredentials(new SymmetricSecurityKey(credentials.GetSigningKey()), SecurityAlgorithms.HmacSha256));
        return new JwtSecurityTokenHandler().WriteToken(token);
    }

    private sealed class ExpiryProbe : Interceptor
    {
        private int _armed;
        private int _admittedCalls;

        internal TaskCompletionSource<IConnectionLifetimeNotificationFeature> Entered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource CloseRequested { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int AdmittedCalls => Volatile.Read(ref _admittedCalls);

        /// <inheritdoc />
        public override async Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            if (Interlocked.CompareExchange(ref _armed, 0, 1) == 1)
            {
                _ = Interlocked.Increment(ref _admittedCalls);
                var lifetime = context.GetHttpContext().Features.GetRequiredFeature<IConnectionLifetimeNotificationFeature>();
                await using var registration = lifetime.ConnectionClosedRequested.Register(() => _ = CloseRequested.TrySetResult());
                _ = Entered.TrySetResult(lifetime);
                await Release.Task.WaitAsync(context.CancellationToken);
            }

            return await continuation(request, context);
        }

        internal void Arm() => Volatile.Write(ref _armed, 1);
    }
}
