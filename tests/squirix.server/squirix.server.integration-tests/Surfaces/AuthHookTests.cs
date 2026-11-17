using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Grpc.Core;
using Grpc.Core.Interceptors;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Grpc;
using Squirix.Transport.Grpc.Cache;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.IntegrationTests.Surfaces;

/// <summary>Verifies authentication extension hooks retain their request and scope semantics.</summary>
public sealed class AuthHookTests : ServerSurfaceTestBase
{
    /// <summary>Honors scoped token-validation denial for both connection and operational authentication.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ScopedEventsCanDenyCredentials(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new HookProbe(true);
        using var port = ListenPortPool.IntegrationTests.HoldPort();
        var origin = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("0.0.0.0", port.Port));
        await using var cluster = await StartClusterAsync(new ClusterNode("hooks", origin), _ => CreateOptions(credentials, probe), cancellationToken);
        var rejectedStatus = byte.MaxValue;
        var loopback = new Uri(NodeInvariantIndexStrings.FormatHttpsOrigin("127.0.0.1", port.Port));
        using var channel = CreateGrpcChannel(loopback, TestJwtHelper.CreateBearerToken(credentials), status => rejectedStatus = status);
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        var denied = await NodeAsyncAssert.ThrowsAsync<RpcException>(client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "denied" }, cancellationToken: cancellationToken).ResponseAsync, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(denied.StatusCode).IsEqualTo(StatusCode.Unavailable);
        _ = await Assert.That(rejectedStatus).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(probe.Events).IsEqualTo(1);
        _ = await Assert.That(probe.RpcEntries).IsEqualTo(0);

        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();
        using var handler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
        using var http = new HttpClient(handler);
        using var request = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp!, port.Port, "/metrics"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        using var response = await http.SendAsync(request, cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        _ = await Assert.That(probe.Events).IsEqualTo(2);
        _ = await Assert.That(probe.ContextErrors).IsEqualTo(0);
        _ = await Assert.That(probe.ScopesDisposed).IsEqualTo(2);
    }

    /// <summary>Preserves token event claims while transforming independent principals in real RPC contexts.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ClaimsTransformOnRealRpcContexts(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new HookProbe(false);
        await using var cluster = await StartClusterAsync("hooks", _ => CreateOptions(credentials, probe), cancellationToken);
        using var channel = CreateGrpcChannel(cluster["hooks"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new SquirixCacheService.SquirixCacheServiceClient(channel);
        for (var i = 0; i < 2; i++)
        {
            var result = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "claims" }, cancellationToken: cancellationToken);
            _ = await Assert.That(result.Found).IsFalse();
        }

        _ = await Assert.That(probe.Events).IsEqualTo(1);
        _ = await Assert.That(probe.ScopesDisposed).IsEqualTo(1);
        _ = await Assert.That(probe.RpcEntries).IsEqualTo(2);
        _ = await Assert.That(probe.Transforms).IsGreaterThanOrEqualTo(2);
        _ = await Assert.That(probe.ContextErrors).IsEqualTo(0);
        _ = await Assert.That(probe.ClaimErrors).IsEqualTo(0);
    }

    private static BlackBoxStartOptions CreateOptions(TestJwtCredentials credentials, HookProbe probe) => new()
    {
        Security = TestNodeSecurityOptions.FromJwtCredentials(credentials),
        ConfigureGrpc = static options => options.Interceptors.Add<ClaimProbeInterceptor>(),
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(probe);
            _ = services.AddHttpContextAccessor();
            _ = services.AddScoped(static provider => new EventScope(provider.GetRequiredService<HookProbe>()));
            _ = services.AddScoped(static provider => new ScopedEvents(provider.GetRequiredService<HookProbe>(), provider.GetRequiredService<EventScope>(), provider.GetRequiredService<IHttpContextAccessor>()));
            _ = services.AddTransient<IClaimsTransformation>(static provider => new RequestClaims(provider.GetRequiredService<HookProbe>(), provider.GetRequiredService<IHttpContextAccessor>()));
            _ = services.AddSingleton(static provider => new ClaimProbeInterceptor(provider.GetRequiredService<HookProbe>()));
            _ = services.PostConfigure<JwtBearerOptions>(JwtBearerDefaults.AuthenticationScheme, static options => options.EventsType = typeof(ScopedEvents));
        },
    };

    private sealed class HookProbe
    {
        private int _events;
        private int _scopesDisposed;
        private int _rpcEntries;
        private int _transforms;
        private int _contextErrors;
        private int _claimErrors;

        internal HookProbe(bool reject)
        {
            Reject = reject;
        }

        internal bool Reject { get; }

        internal int Events => Volatile.Read(ref _events);

        internal int ScopesDisposed => Volatile.Read(ref _scopesDisposed);

        internal int RpcEntries => Volatile.Read(ref _rpcEntries);

        internal int Transforms => Volatile.Read(ref _transforms);

        internal int ContextErrors => Volatile.Read(ref _contextErrors);

        internal int ClaimErrors => Volatile.Read(ref _claimErrors);

        internal void RecordEvents() => _ = Interlocked.Increment(ref _events);

        internal void RecordScopesDisposed() => _ = Interlocked.Increment(ref _scopesDisposed);

        internal void RecordRpcEntries() => _ = Interlocked.Increment(ref _rpcEntries);

        internal void RecordTransforms() => _ = Interlocked.Increment(ref _transforms);

        internal void RecordContextErrors() => _ = Interlocked.Increment(ref _contextErrors);

        internal void RecordClaimErrors() => _ = Interlocked.Increment(ref _claimErrors);
    }

    private sealed class EventScope : IDisposable
    {
        private readonly HookProbe _probe;
        private bool _validated;

        public EventScope(HookProbe probe)
        {
            _probe = probe;
        }

        public void Dispose()
        {
            if (_validated)
                _probe.RecordScopesDisposed();
        }

        internal void MarkValidated() => _validated = true;
    }

    private sealed class ScopedEvents : JwtBearerEvents
    {
        private readonly HookProbe _probe;
        private readonly EventScope _scope;
        private readonly IHttpContextAccessor _accessor;

        public ScopedEvents(HookProbe probe, EventScope scope, IHttpContextAccessor accessor)
        {
            _probe = probe;
            _scope = scope;
            _accessor = accessor;
        }

        /// <inheritdoc />
        public override Task TokenValidated(TokenValidatedContext context)
        {
            _scope.MarkValidated();
            _probe.RecordEvents();
            if (!ReferenceEquals(context.HttpContext, _accessor.HttpContext) || !ReferenceEquals(_scope, context.HttpContext.RequestServices.GetRequiredService<EventScope>()))
                _probe.RecordContextErrors();
            if (_probe.Reject)
                context.Fail("Rejected by the configured scoped validation event.");
            else
                context.Principal!.AddIdentity(new ClaimsIdentity([new Claim("token-event", "retained")]));
            return Task.CompletedTask;
        }
    }

    private sealed class RequestClaims : IClaimsTransformation
    {
        private readonly HookProbe _probe;
        private readonly IHttpContextAccessor _accessor;

        public RequestClaims(HookProbe probe, IHttpContextAccessor accessor)
        {
            _probe = probe;
            _accessor = accessor;
        }

        public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
        {
            _probe.RecordTransforms();
            if (!string.Equals(_accessor.HttpContext?.Request.Method, "POST", StringComparison.Ordinal))
                _probe.RecordContextErrors();
            if (principal.HasClaim(static claim => string.Equals(claim.Type, "request-transform", StringComparison.Ordinal)) || !principal.HasClaim("token-event", "retained"))
                _probe.RecordClaimErrors();
            principal.AddIdentity(new ClaimsIdentity([new Claim("request-transform", "fresh")]));
            return Task.FromResult(principal);
        }
    }

    private sealed class ClaimProbeInterceptor : Interceptor
    {
        private readonly HookProbe _probe;

        public ClaimProbeInterceptor(HookProbe probe)
        {
            _probe = probe;
        }

        /// <inheritdoc />
        public override Task<TResponse> UnaryServerHandler<TRequest, TResponse>(TRequest request, ServerCallContext context, UnaryServerMethod<TRequest, TResponse> continuation)
        {
            _probe.RecordRpcEntries();
            var principal = context.GetHttpContext().User;
            var count = 0;
            foreach (var claim in principal.Claims)
            {
                if (string.Equals(claim.Type, "request-transform", StringComparison.Ordinal))
                    count++;
            }

            if (count != 1 || !principal.HasClaim("token-event", "retained"))
                _probe.RecordClaimErrors();
            return continuation(request, context);
        }
    }
}
