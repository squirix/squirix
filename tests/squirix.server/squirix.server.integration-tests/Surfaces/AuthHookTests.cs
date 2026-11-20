using System;
using System.Net;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Hosting;
using Squirix.Server.TestKit.Networking;
using Squirix.Transport.Binary;
using Squirix.Transport.Wire;
using Squirix.Transport.Wire.Cache;
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
        using var channel = CreateBinaryChannel(loopback, TestJwtHelper.CreateBearerToken(credentials), status => rejectedStatus = status);
        var client = new CacheWireClient(channel);
        var denied = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "denied" }, cancellationToken: cancellationToken), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
        _ = await Assert.That(denied.StatusCode).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(rejectedStatus).IsEqualTo(ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(probe.Events).IsEqualTo(1);
        _ = await Assert.That(probe.RpcEntries).IsEqualTo(0);

        var localIp = LocalHostNetworking.GetLocalNonLoopbackIpv4();
        _ = await Assert.That(string.IsNullOrWhiteSpace(localIp)).IsFalse();
        using var handler = LoopbackHttp.CreateHandlerAllowingCertNameMismatch();
        using var http = new HttpClient(handler);
        using var request = new HttpRequestMessage(HttpMethod.Get, NodeInvariantIndexStrings.FormatHttpsAbsolute(localIp!, cluster["hooks"].ManagementUri.Port, "/metrics"));
        request.Headers.Authorization = new AuthenticationHeaderValue("Bearer", TestJwtHelper.CreateBearerToken(credentials));
        using var response = await http.SendAsync(request, cancellationToken);
        _ = await Assert.That(response.StatusCode).IsEqualTo(HttpStatusCode.Unauthorized);
        _ = await Assert.That(probe.Events).IsEqualTo(2);
        _ = await Assert.That(probe.ContextErrors).IsEqualTo(0);
        await probe.BothScopesDisposed.WaitAsync(TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);
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
        using var channel = CreateBinaryChannel(cluster["hooks"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new CacheWireClient(channel);
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

    /// <summary>An exception from one call maps to Unknown and preserves the authenticated connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedHookKeepsConnectionUsable(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new HookProbe(false);
        await using var cluster = await StartClusterAsync("hooks", _ => CreateOptions(credentials, probe), cancellationToken);
        using var channel = CreateBinaryChannel(cluster["hooks"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new CacheWireClient(channel);
        var request = new GetValueAsyncRequest { CacheName = "default", Key = "hook-failure" };

        probe.FailNextTransform();
        var failure = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.GetValueAsync(request, cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(failure.StatusCode).IsEqualTo(FrameStatus.Unknown);

        var result = await client.GetValueAsync(request, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
        _ = await Assert.That(probe.Events).IsEqualTo(1);
        _ = await Assert.That(probe.RpcEntries).IsEqualTo(1);
    }

    private static BlackBoxStartOptions CreateOptions(TestJwtCredentials credentials, HookProbe probe) => new()
    {
        ManagementUri = new Uri("https://0.0.0.0:0"),
        MetricsEnabled = true,
        PrometheusMetricsEnabled = true,
        Security = TestNodeSecurityOptions.FromJwtCredentials(credentials),
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(probe);
            _ = services.AddScoped(static provider => new EventScope(provider.GetRequiredService<HookProbe>()));
            _ = services.AddScoped<IConnectionTokenObserver>(static provider => new ScopedEvents(provider.GetRequiredService<HookProbe>(), provider.GetRequiredService<EventScope>()));
            _ = services.AddTransient<IBinaryAuthorizationHook>(static provider => new RequestClaims(provider.GetRequiredService<HookProbe>(), provider.GetRequiredService<BinaryRequestAccessor>()));
            _ = services.AddSingleton(static provider => new BinaryCallObserver(new ClaimProbeInterceptor(provider.GetRequiredService<HookProbe>()).Observe));
        },
    };

    private sealed class HookProbe
    {
        private readonly TaskCompletionSource _bothScopesDisposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private int _events;
        private int _scopesDisposed;
        private int _rpcEntries;
        private int _transforms;
        private int _contextErrors;
        private int _claimErrors;
        private int _failNextTransform;

        internal HookProbe(bool reject)
        {
            Reject = reject;
        }

        internal bool Reject { get; }

        internal int Events => Volatile.Read(ref _events);

        internal int ScopesDisposed => Volatile.Read(ref _scopesDisposed);

        internal Task BothScopesDisposed => _bothScopesDisposed.Task;

        internal int RpcEntries => Volatile.Read(ref _rpcEntries);

        internal int Transforms => Volatile.Read(ref _transforms);

        internal int ContextErrors => Volatile.Read(ref _contextErrors);

        internal int ClaimErrors => Volatile.Read(ref _claimErrors);

        internal void FailNextTransform() => Volatile.Write(ref _failNextTransform, 1);

        internal bool TakeTransformFailure() => Interlocked.Exchange(ref _failNextTransform, 0) != 0;

        internal void RecordEvents() => _ = Interlocked.Increment(ref _events);

        internal void RecordScopesDisposed()
        {
            if (Interlocked.Increment(ref _scopesDisposed) == 2)
                _ = _bothScopesDisposed.TrySetResult();
        }

        internal void RecordRpcEntries() => _ = Interlocked.Increment(ref _rpcEntries);

        internal void RecordTransforms() => _ = Interlocked.Increment(ref _transforms);

        internal void RecordContextErrors() => _ = Interlocked.Increment(ref _contextErrors);

        internal void RecordClaimErrors() => _ = Interlocked.Increment(ref _claimErrors);
    }

    private sealed class EventScope : IDisposable
    {
        private readonly HookProbe _probe;
        private bool _validated;

        internal EventScope(HookProbe probe)
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

    private sealed class ScopedEvents : IConnectionTokenObserver
    {
        private readonly HookProbe _probe;
        private readonly EventScope _scope;
        internal ScopedEvents(HookProbe probe, EventScope scope)
        {
            _probe = probe;
            _scope = scope;
        }

        /// <inheritdoc />
        public ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            _scope.MarkValidated();
            _probe.RecordEvents();
            if (!ReferenceEquals(_scope, services.GetRequiredService<EventScope>()) ||
                (_probe.Events > 1 && connection != null))
                _probe.RecordContextErrors();
            if (_probe.Reject)
                return ValueTask.FromResult<ClaimsPrincipal?>(null);

            var identity = new ClaimsIdentity([new Claim("token-event", "retained")]);
            identity.FindFirst("token-event")!.Properties["state"] = "original";
            principal.AddIdentity(identity);
            return ValueTask.FromResult<ClaimsPrincipal?>(principal);
        }
    }

    private sealed class RequestClaims : IBinaryAuthorizationHook
    {
        private readonly HookProbe _probe;
        private readonly BinaryRequestAccessor _accessor;

        internal RequestClaims(HookProbe probe, BinaryRequestAccessor accessor)
        {
            _probe = probe;
            _accessor = accessor;
        }

        public ValueTask<FrameStatus> AuthorizeAsync(BinaryRequestContext request)
        {
            _probe.RecordTransforms();
            if (_probe.TakeTransformFailure())
                return ValueTask.FromException<FrameStatus>(new InvalidOperationException("Scripted claims transformation failure."));

            var principal = request.User!;
            var connection = request.Connection;
            if (!ReferenceEquals(request, _accessor.Current) || !request.Path.StartsWith('/') ||
                connection?.RemoteEndPoint is not { Port: > 0 } remote ||
                !IPAddress.IsLoopback(remote.Address.IsIPv4MappedToIPv6 ? remote.Address.MapToIPv4() : remote.Address) ||
                connection.LocalEndPoint is not { Port: > 0 })
                _probe.RecordContextErrors();
            if (principal.HasClaim(static claim => string.Equals(claim.Type, "request-transform", StringComparison.Ordinal)) || !principal.HasClaim("token-event", "retained"))
                _probe.RecordClaimErrors();
            foreach (var identity in principal.Identities)
            {
                var claim = identity.FindFirst("token-event");
                if (claim == null)
                    continue;
                if (!claim.Properties.TryGetValue("state", out var state) || !string.Equals(state, "original", StringComparison.Ordinal))
                    _probe.RecordClaimErrors();
                claim.Properties["state"] = "request";
                identity.AddClaim(new Claim("request-transform", "fresh"));
                break;
            }

            return ValueTask.FromResult(FrameStatus.Ok);
        }
    }

    private sealed class ClaimProbeInterceptor
    {
        private readonly HookProbe _probe;

        internal ClaimProbeInterceptor(HookProbe probe)
        {
            _probe = probe;
        }

        internal void Observe(BinaryRequestContext context)
        {
            _probe.RecordRpcEntries();
            var principal = context.User!;
            var count = 0;
            foreach (var claim in principal.Claims)
            {
                if (string.Equals(claim.Type, "request-transform", StringComparison.Ordinal))
                    count++;
            }

            if (count != 1 || !principal.HasClaim("token-event", "retained"))
                _probe.RecordClaimErrors();
        }
    }
}
