using System;
using System.Security.Claims;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;
using Microsoft.Extensions.Time.Testing;
using Microsoft.IdentityModel.Protocols;
using Microsoft.IdentityModel.Protocols.OpenIdConnect;
using Squirix.Server.Adapters.Endpoint;
using Squirix.Server.Attributes;
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

    private static async Task<bool> AdmitAfterLookupAsync(int change, CancellationToken cancellationToken)
    {
        var services = new ServiceCollection();
        _ = services.AddOptions();
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IOptionsMonitorCache<JwtValidationOptions>>();
        var monitor = provider.GetRequiredService<IOptionsMonitor<JwtValidationOptions>>();
        var gate = new ConfigurationGate();
        var options = new JwtValidationOptions { ConfigurationManager = gate };
        _ = await Assert.That(cache.TryAdd(Options.DefaultName, options)).IsTrue();
        var configuration = new OpenIdConnectConfiguration();
        var clock = new FakeTimeProvider(DateTimeOffset.UnixEpoch);
        var connection = AuthenticatedConnection.Create(new ClaimsPrincipal(), (DateTime.UnixEpoch, DateTime.UnixEpoch.AddSeconds(10)), options, configuration, null, static () => { });
        using var validator = new NativeConnectionAuthenticationValidator(monitor, clock, provider.GetRequiredService<IServiceScopeFactory>());
        var pending = validator.IsCurrentAsync(connection!, cancellationToken);
        await gate.Requested.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        _ = await Assert.That(pending.IsCompleted).IsFalse();
        if (change == 0)
        {
            clock.Advance(TimeSpan.FromSeconds(10));
        }
        else if (change == 1)
        {
            _ = await Assert.That(cache.TryRemove(Options.DefaultName)).IsTrue();
            _ = await Assert.That(cache.TryAdd(Options.DefaultName, new JwtValidationOptions { ConfigurationManager = gate })).IsTrue();
        }
        else
        {
            clock.Advance(TimeSpan.FromSeconds(9));
        }

        gate.Complete(configuration);
        return await pending;
    }

    private sealed class ConfigurationGate : IConfigurationManager<OpenIdConnectConfiguration>
    {
        private readonly TaskCompletionSource _requested = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource<OpenIdConnectConfiguration> _result = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal Task Requested => _requested.Task;

        public Task<OpenIdConnectConfiguration> GetConfigurationAsync(CancellationToken cancel)
        {
            _ = _requested.TrySetResult();
            return _result.Task.WaitAsync(cancel);
        }

        public void RequestRefresh()
        {
        }

        internal void Complete(OpenIdConnectConfiguration configuration) => _ = _result.TrySetResult(configuration);
    }
}
