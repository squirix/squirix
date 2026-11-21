using System;
using System.IO;
using System.Net;
using System.Security.Claims;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Composition;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.Utils;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Unit tests for native management loopback-or-bearer access control.</summary>
[Immutable]
public sealed class MetricsAuthOrLoopbackFilterTests
{
    /// <summary>A cookie header does not authorize a remote diagnostics request.</summary>
    [Test]
    public async Task HostCookieDoesNotAuthorizeRemoteRequest()
    {
        await using var services = CreateServices(TestJwtHelper.CreateRandomCredentials());
        var endpoint = CreateEndpoint(services);
        var stream = new MemoryStream();
        await using var lifetime = stream.ConfigureAwait(false);
        const string request = "GET /health/ready/details HTTP/1.1\r\nHost: test\r\nCookie: .AspNetCore.Cookies=opaque\r\n\r\n";
        var buffer = Encoding.ASCII.GetBytes(request);
        await stream.WriteAsync(buffer, CancellationToken.None);
        stream.Position = 0;

        await endpoint.ServeAsync(stream, new IPEndPoint(RemoteAddress(), 1234), CancellationToken.None, CancellationToken.None);
        var response = Encoding.ASCII.GetString(stream.GetBuffer(), buffer.Length, int.CreateChecked(stream.Position) - buffer.Length);
        _ = await Assert.That(response.StartsWith("HTTP/1.1 401 Unauthorized\r\n", StringComparison.Ordinal)).IsTrue();
    }

    /// <summary>Verifies loopback clients can scrape metrics without authentication.</summary>
    /// <param name="address">The loopback address.</param>
    [Test]
    [Arguments("127.0.0.1")]
    [Arguments("::1")]
    [Arguments("::ffff:127.0.0.1")]
    public async Task LoopbackAllowedWithoutAuthentication(string address)
    {
        await using var services = CreateServices(null);
        var endpoint = CreateEndpoint(services);
        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(IPAddress.Parse(address), null, CancellationToken.None)).IsTrue();
    }

    /// <summary>A missing connection address is not treated as loopback.</summary>
    [Test]
    public async Task MissingAddressRequiresBearer()
    {
        await using var services = CreateServices(null);
        _ = await Assert.That(await CreateEndpoint(services).CanAccessDetailsAsync(null, null, CancellationToken.None)).IsFalse();
    }

    /// <summary>Options reloaded while the scoped observer is disposed invalidate the current authentication.</summary>
    [Test]
    public async Task RejectsReloadDuringObserverDisposal()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var services = CreateServices(
            credentials,
            static registrations =>
            {
                var original = registrations.FindLast(static entry => entry.ServiceType == typeof(Func<JwtValidationOptions>));
                var configured = ThrowHelper.Required(original?.ImplementationInstance as Func<JwtValidationOptions>, "JWT options were not registered.");
                var current = configured();
                _ = registrations.AddSingleton<Func<JwtValidationOptions>>(() => Volatile.Read(ref current));
                _ = registrations.AddScoped<IConnectionTokenObserver>(_ =>
                    new OptionsResetObserver(() =>
                    {
                        var source = configured();
                        Volatile.Write(ref current, new JwtValidationOptions
                        {
                            Audience = source.Audience,
                            Issuer = source.Issuer,
                            SigningKey = source.SigningKey,
                        });
                    }));
            });
        var endpoint = CreateEndpoint(services);
        var token = TestJwtHelper.CreateBearerToken(credentials);
        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(RemoteAddress(), token, CancellationToken.None)).IsFalse();
    }

    /// <summary>Remote management denies a JWT when the observer returns an unauthenticated identity.</summary>
    [Test]
    public async Task RejectsUnauthenticatedObserverPrincipal()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var services = CreateServices(
            credentials,
            static registrations => registrations.AddScoped<IConnectionTokenObserver>(static _ => new UnauthenticatedObserver()));
        var endpoint = CreateEndpoint(services);
        var token = TestJwtHelper.CreateBearerToken(credentials);
        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(RemoteAddress(), token, CancellationToken.None)).IsFalse();
    }

    /// <summary>Verifies a valid operational bearer token permits a remote scrape.</summary>
    [Test]
    public async Task RemoteAllowedWhenAuthenticated()
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        await using var services = CreateServices(credentials);
        var endpoint = CreateEndpoint(services);
        var token = TestJwtHelper.CreateBearerToken(credentials);

        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(RemoteAddress(), token, CancellationToken.None)).IsTrue();
    }

    /// <summary>A bearer token cannot authorize a request if JWT validation is unconfigured.</summary>
    [Test]
    public async Task RemoteRejectedWithoutBearerRegistration()
    {
        await using var services = CreateServices(null);
        var endpoint = CreateEndpoint(services);
        var token = TestJwtHelper.CreateBearerToken(TestJwtHelper.CreateRandomCredentials());
        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(RemoteAddress(), token, CancellationToken.None)).IsFalse();
    }

    /// <summary>Verifies remote unauthenticated clients are rejected.</summary>
    /// <param name="token">The invalid or absent bearer token.</param>
    [Test]
    [Arguments("")]
    [Arguments("invalid.jwt.token")]
    public async Task UnauthenticatedRemoteRejected(string token)
    {
        await using var services = CreateServices(TestJwtHelper.CreateRandomCredentials());
        var endpoint = CreateEndpoint(services);
        _ = await Assert.That(await endpoint.CanAccessDetailsAsync(RemoteAddress(), token, CancellationToken.None)).IsFalse();
    }

    private static NativeManagementEndpoint CreateEndpoint(IServiceProvider services)
    {
        var validator = services.GetService<NativeConnectionAuthenticationValidator>();
        Func<string, CancellationToken, ValueTask<bool>> authenticate = validator == null ? static (_, _) => ValueTask.FromResult(false) : validator.ValidateBearerAsync;
        return new NativeManagementEndpoint(services, authenticate, NullLogger<NativeManagementEndpoint>.Instance);
    }

    private static NodeProvider CreateServices(TestJwtCredentials? credentials, Action<TestServiceCollection>? configure = null)
    {
        var services = new TestServiceCollection();
        var options = new SecurityOptions
        {
            JwtAudience = credentials?.Audience,
            JwtIssuer = credentials?.Issuer,
            JwtSigningKey = credentials == null ? null : Convert.ToBase64String(credentials.GetSigningKey()),
        };

        _ = services.AddSquirixSecurityServices(options);
        configure?.Invoke(services);
        return services.BuildServiceProvider();
    }

    private static IPAddress RemoteAddress() => IPAddress.Parse("203.0.113.10");

    private sealed class OptionsResetObserver : IConnectionTokenObserver, IAsyncDisposable
    {
        private readonly Action _reset;

        internal OptionsResetObserver(Action reset)
        {
            _reset = reset;
        }

        public ValueTask DisposeAsync()
        {
            _reset();
            return ValueTask.CompletedTask;
        }

        public ValueTask<ClaimsPrincipal?> OnValidatedAsync(
            ClaimsPrincipal principal,
            IServiceProvider services,
            BinaryConnection? connection,
            CancellationToken cancellationToken) => ValueTask.FromResult<ClaimsPrincipal?>(principal);
    }

    private sealed class UnauthenticatedObserver : IConnectionTokenObserver
    {
        public ValueTask<ClaimsPrincipal?>
            OnValidatedAsync(ClaimsPrincipal principal, IServiceProvider services, BinaryConnection? connection, CancellationToken cancellationToken) =>
            ValueTask.FromResult<ClaimsPrincipal?>(new ClaimsPrincipal(new ClaimsIdentity()));
    }
}
