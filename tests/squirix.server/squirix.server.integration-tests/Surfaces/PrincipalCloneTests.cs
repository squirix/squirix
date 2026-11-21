using System;
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

/// <summary>Exercises custom principal semantics and fail-closed cloning before real cache mutations.</summary>
public sealed class PrincipalCloneTests : ServerSurfaceTestBase
{
    /// <summary>Role and claim overrides survive the handshake snapshot and every real RPC clone.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CustomAuthorizationReachesRpc(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new PolicyProbe(true);
        await using var cluster = await StartClusterAsync("clone-role", _ => CreateOptions(credentials, probe, 0), cancellationToken);
        using var channel = CreateBinaryChannel(cluster["clone-role"].Uri, TestJwtHelper.CreateBearerToken(credentials));
        var client = new CacheWireClient(channel);
        _ = await client.SetEntryAsync(CreateMutation("clone-role"), cancellationToken: cancellationToken);
        var result = await client.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "clone-role" }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsTrue();
        _ = await Assert.That(result.Value.StringValue).IsEqualTo("preserved");
        _ = await Assert.That(probe.Entries).IsEqualTo(2);
        _ = await Assert.That(probe.Errors).IsEqualTo(0);
    }

    /// <summary>Malformed handshake and request clones are denied before a mutation reaches the endpoint.</summary>
    /// <param name="duringRequest">Whether the clone fails after a valid handshake snapshot.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task MalformedClonesCannotMutate(bool duringRequest, CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var probe = new PolicyProbe(false);
        await using var cluster = await StartClusterAsync("clone-denial", _ => CreateOptions(credentials, probe, duringRequest ? 2 : 1), cancellationToken);
        var uri = cluster["clone-denial"].Uri;
        var acknowledgement = byte.MaxValue;
        using var channel = CreateBinaryChannel(uri, TestJwtHelper.CreateBearerToken(credentials), status => acknowledgement = status);
        var client = new CacheWireClient(channel);
        var exception = await NodeAsyncAssert.ThrowsAsync<ServerCallException>(
            client.SetEntryAsync(CreateMutation("clone-denial"), cancellationToken: cancellationToken),
            TimeSpan.FromSeconds(30),
            TimeProvider.System,
            cancellationToken);
        _ = await Assert.That(acknowledgement).IsEqualTo(duringRequest ? ConnectionAuthenticationProtocol.Accepted : ConnectionAuthenticationProtocol.InvalidCredentials);
        _ = await Assert.That(exception.StatusCode).IsEqualTo(duringRequest ? FrameStatus.Unauthenticated : FrameStatus.Unavailable);
        _ = await Assert.That(probe.Entries).IsEqualTo(0);
        probe.EnableControl();
        using var control = CreateBinaryChannel(uri, TestJwtHelper.CreateBearerToken(credentials));
        var controlClient = new CacheWireClient(control);
        var result = await controlClient.GetValueAsync(new GetValueAsyncRequest { CacheName = "default", Key = "clone-denial" }, cancellationToken: cancellationToken);
        _ = await Assert.That(result.Found).IsFalse();
        _ = await Assert.That(probe.Entries).IsEqualTo(1);
    }

    private static SetEntryAsyncRequest CreateMutation(string key) => new()
    {
        CacheName = "default",
        Key = key,
        OperationId = SurfaceMutationOpIds.Default,
        Entry = new CacheEntryWire { Value = new CacheValue { StringValue = "preserved" } },
    };

    private static BlackBoxStartOptions CreateOptions(TestJwtCredentials credentials, PolicyProbe probe, int mode) => new()
    {
        Security = TestNodeSecurityOptions.FromJwtCredentials(credentials),
        ServicesConfigure = services =>
        {
            _ = services.AddSingleton(static provider => new BinaryCallObserver(new PolicyInterceptor(provider.GetRequiredService<PolicyProbe>()).Observe));
            _ = services.AddSingleton(probe);
            _ = services.AddSingleton<IConnectionTokenObserver>(new PrincipalFactory(probe, mode));
        },
    };

    private sealed class ChangingPrincipal : ClaimsPrincipal
    {
        private readonly bool _allowClone;
        private readonly ClaimsIdentity _identity;

        internal ChangingPrincipal(ClaimsIdentity identity, bool allowClone)
            : base(identity)
        {
            _identity = identity;
            _allowClone = allowClone;
        }

        public override ClaimsPrincipal Clone() => _allowClone ? new ChangingPrincipal(_identity.Clone(), false) : this;
    }

    private sealed class PolicyInterceptor
    {
        private readonly PolicyProbe _probe;

        internal PolicyInterceptor(PolicyProbe probe)
        {
            _probe = probe;
        }

        internal void Observe(BinaryRequestContext context)
        {
            _probe.RecordEntry();
            var principal = context.User!;
            if (_probe.CheckPolicy && (principal.IsInRole("admin") || principal.HasClaim("permission", "grant") || !string.Equals(principal.FindFirst("permission")?.Value, "grant", StringComparison.Ordinal)))
                _probe.RecordError();
        }
    }

    private sealed class PolicyPrincipal : ClaimsPrincipal
    {
        private readonly ClaimsIdentity _identity;

        internal PolicyPrincipal(ClaimsIdentity identity)
            : base(identity)
        {
            _identity = identity;
        }

        public override ClaimsPrincipal Clone() => new PolicyPrincipal(_identity.Clone());

        public override bool HasClaim(string type, string value) => false;

        public override bool IsInRole(string role) => false;
    }

    private sealed class PolicyProbe
    {
        private int _allowControl;
        private int _entries;
        private int _errors;

        internal PolicyProbe(bool checkPolicy)
        {
            CheckPolicy = checkPolicy;
        }

        internal bool AllowControl => Volatile.Read(ref _allowControl) != 0;

        internal bool CheckPolicy { get; }

        internal int Entries => Volatile.Read(ref _entries);

        internal int Errors => Volatile.Read(ref _errors);

        internal void EnableControl() => _ = Interlocked.Exchange(ref _allowControl, 1);

        internal void RecordEntry() => _ = Interlocked.Increment(ref _entries);

        internal void RecordError() => _ = Interlocked.Increment(ref _errors);
    }

    private sealed class PrincipalFactory : IConnectionTokenObserver
    {
        private readonly int _mode;
        private readonly PolicyProbe _probe;

        internal PrincipalFactory(PolicyProbe probe, int mode)
        {
            _probe = probe;
            _mode = mode;
        }

        public ValueTask<ClaimsPrincipal?> OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            if (_probe.AllowControl)
                return ValueTask.FromResult<ClaimsPrincipal?>(principal);
            var identity = new ClaimsIdentity([new Claim(ClaimTypes.Role, "admin"), new Claim("permission", "grant")], "Bearer");
            principal = _mode switch
            {
                0 => new PolicyPrincipal(identity),
                1 => new UnsupportedPrincipal(identity),
                _ => new ChangingPrincipal(identity, true),
            };

            return ValueTask.FromResult<ClaimsPrincipal?>(principal);
        }
    }

    private sealed class UnsupportedPrincipal : ClaimsPrincipal
    {
        internal UnsupportedPrincipal(ClaimsIdentity identity)
            : base(identity)
        {
        }
    }
}
