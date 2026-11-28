using System;
using System.IO;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
using Squirix.Server.Node.Backpressure;
using Squirix.Server.TestKit;
using Squirix.Testing;
using Squirix.Transport.Binary;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Checks admission bounds and options generation again after an asynchronous metadata lookup.</summary>
[Immutable]
public sealed class ConnectionAdmissionTests
{
    /// <summary>A metadata lookup cannot admit a connection that expired while it was suspended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ExpiredDuringLookupIsRejected(CancellationToken cancellationToken) =>
        _ = await Assert.That(await AdmitAfterLookupAsync(0, cancellationToken)).IsFalse();

    /// <summary>A metadata lookup cannot retain options that were replaced while it was suspended.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ReloadDuringLookupIsRejected(CancellationToken cancellationToken) =>
        _ = await Assert.That(await AdmitAfterLookupAsync(1, cancellationToken)).IsFalse();

    /// <summary>A suspended lookup still admits the same trusted configuration within the lifetime bounds.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task UnchangedLookupWithinBoundsIsAccepted(CancellationToken cancellationToken) =>
        _ = await Assert.That(await AdmitAfterLookupAsync(2, cancellationToken)).IsTrue();

    /// <summary>A metadata manager can supply the issuer and signing key without inheriting the IdentityModel base class.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task AcceptsIssuerAndKeyFromNonBaseManager(CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var configuration = new JwtMetadata { Issuer = credentials.Issuer };
        configuration.SigningKeys.Add(new JwtVerificationKey(credentials.GetSigningKey()));
        var gate = new ConfigurationGate();
        gate.Complete(configuration);

        await using var provider = new TestServiceCollection().BuildServiceProvider();
        var options = new JwtValidationOptions
        {
            Metadata = gate,
            Audience = credentials.Audience,
        };
        using var validator = new NativeConnectionAuthenticationValidator(() => options, TimeProvider.System, provider);
        var token = TestJwtHelper.CreateBearerToken(credentials);
        _ = await Assert.That(await validator.ValidateBearerAsync(token, cancellationToken)).IsTrue();
    }

    /// <summary>Only callers with per-client limits need a request identity on the standard JWT path.</summary>
    /// <param name="perClient">Whether admission uses the authenticated subject.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task JwtRequestIdentityFollowsAdmission(bool perClient, CancellationToken cancellationToken)
    {
        var credentials = TestJwtHelper.CreateRandomCredentials();
        var options = new JwtValidationOptions
        {
            Issuer = credentials.Issuer,
            Audience = credentials.Audience,
            SigningKey = new JwtVerificationKey(credentials.GetSigningKey()),
        };
        IBackpressureClientIdResolver clients = perClient ? new BinaryClientIdResolver() : SharedClientIdResolver.Instance;
        await using var services = new TestServiceCollection().AddSingleton(clients).BuildServiceProvider();
        using var validator = new NativeConnectionAuthenticationValidator(() => options, TimeProvider.System, services);
        using var connection = new BinaryConnection("test", new NativeBinaryEndpoint.StreamPipe(Stream.Null), null, null, cancellationToken, cancellationToken);
        var token = TestJwtHelper.CreateBearerToken(credentials, "tenant-a");
        var identity = await validator.ValidateAsync(token, connection, cancellationToken);
        var request = new BinaryRequestContext { Path = "/GetDistributed" };
        var status = await validator.AuthorizeAsync(identity!, request, cancellationToken);
        _ = await Assert.That(status).IsEqualTo(FrameStatus.Ok);
        _ = await Assert.That(request.User != null).IsEqualTo(perClient);
        var clientId = perClient ? BinaryClientIdResolver.ResolveRequest(request) : clients.Resolve();
        _ = await Assert.That(clientId).IsEqualTo(perClient ? "jwt:tenant-a" : ClientIds.Runtime);

        options = new JwtValidationOptions();
        _ = await Assert.That(await validator.AuthorizeAsync(identity!, request, cancellationToken)).IsEqualTo(FrameStatus.Unavailable);
        _ = await Assert.That(connection.CloseRequested.IsCancellationRequested).IsTrue();
    }

    private static async Task<bool> AdmitAfterLookupAsync(int change, CancellationToken cancellationToken)
    {
        await using var provider = new TestServiceCollection().BuildServiceProvider();
        var gate = new ConfigurationGate();
        var options = new JwtValidationOptions { Metadata = gate };
        var configuration = new JwtMetadata();
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(), (DateTime.UnixEpoch, DateTime.UnixEpoch.AddSeconds(10)), options, configuration, null, static () => { });
        using var validator = new NativeConnectionAuthenticationValidator(() => Volatile.Read(ref options), clock, provider);
        var pending = validator.IsCurrentAsync(connection!, cancellationToken);
        await gate.Requested.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        if (change == 0)
            clock.Advance(TimeSpan.FromSeconds(10));
        else if (change == 1)
            Volatile.Write(ref options, new JwtValidationOptions { Metadata = gate });
        else
            clock.Advance(TimeSpan.FromSeconds(9));

        gate.Complete(configuration);
        return await pending;
    }

    private sealed class ConfigurationGate : IJwtMetadataSource
    {
        private readonly TaskCompletionSource _requested = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<JwtMetadata> _result = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public bool UseLastKnownGoodConfiguration => false;

        public TimeSpan LastKnownGoodLifetime => TimeSpan.Zero;

        internal Task Requested => _requested.Task;

        public Task<JwtMetadata> GetConfigurationAsync(CancellationToken cancellationToken)
        {
            _ = _requested.TrySetResult();
            return _result.Task.WaitAsync(cancellationToken);
        }

        public void RequestRefresh()
        {
        }

        internal void Complete(JwtMetadata configuration) => _ = _result.TrySetResult(configuration);
    }
}
