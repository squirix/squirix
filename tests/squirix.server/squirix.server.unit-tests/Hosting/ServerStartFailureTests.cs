using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that a failed or cancelled server startup stops and releases the application and surfaces the original failure.</summary>
public sealed class ServerStartFailureTests
{
    /// <summary>A hosted service that fails to start leaves no running service and the original exception reaches the caller.</summary>
    [Test]
    public async Task FailedStartStopsStartedServices()
    {
        var probe = new StartProbe();
        var failure = new InvalidOperationException("Probe start failure.");
        var app = BuildApp(probe, failure);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartAppAsync(app, static _ => { }, CancellationToken.None));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    /// <summary>A cancelled start disposes the application and surfaces the cancellation.</summary>
    [Test]
    public async Task CancelledStartDisposesApplication()
    {
        var probe = new StartProbe();
        var app = BuildApp(probe, null);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(StartAppAsync(app, static _ => { }, cts.Token));

        _ = await Assert.That(IsContainerDisposed(app)).IsTrue();
    }

    /// <summary>A failure before startup begins disposes the built application and rethrows the original exception.</summary>
    [Test]
    public async Task FailedConfigureDisposesApplication()
    {
        var probe = new StartProbe();
        var failure = new InvalidOperationException("Configure failure.");
        var app = BuildApp(probe, null);

        var start = StartAppAsync(app, _ => throw failure, CancellationToken.None);
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(start);

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(IsContainerDisposed(app)).IsTrue();
    }

    /// <summary>A cleanup failure is logged and never replaces the original startup exception.</summary>
    [Test]
    public async Task CleanupFailureDoesNotReplaceStartFailure()
    {
        var probe = new StartProbe { ThrowOnDependencyDispose = true };
        var failure = new InvalidOperationException("Probe start failure.");
        var app = BuildApp(probe, failure);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartAppAsync(app, static _ => { }, CancellationToken.None));

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    private static Task<SquirixServer.ApplicationHandle> StartAppAsync(WebApplication app, Action<WebApplication> configure, CancellationToken cancellationToken) =>
        SquirixServer.ApplicationHandle.StartApplicationAsync(app, configure, cancellationToken).AsTask();

    private static bool IsContainerDisposed(WebApplication app)
    {
        try
        {
            _ = app.Services.GetService<StartProbe>();
            return false;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    private static WebApplication BuildApp(StartProbe probe, Exception? startFailure)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = [] });
        _ = builder.WebHost.UseUrls("http://127.0.0.1:0");
        _ = builder.Logging.ClearProviders();
        _ = builder.Services.AddSingleton(probe);
        _ = builder.Services.AddSingleton(static serviceProvider => new ProbeDependency(serviceProvider.GetRequiredService<StartProbe>()));
        _ = builder.Services.AddSingleton<IHostedService>(serviceProvider =>
        {
            _ = serviceProvider.GetRequiredService<ProbeDependency>();
            return new RunningService(probe);
        });
        if (startFailure != null)
            _ = builder.Services.AddSingleton<IHostedService>(_ => new FailingService(startFailure));

        return builder.Build();
    }

    private sealed class StartProbe
    {
        private int _stopCalls;
        private volatile bool _dependencyDisposed;

        internal int StopCalls => Volatile.Read(ref _stopCalls);

        internal bool DependencyDisposed
        {
            get => _dependencyDisposed;
            set => _dependencyDisposed = value;
        }

        internal bool ThrowOnDependencyDispose { get; init; }

        internal void RecordStop() => _ = Interlocked.Increment(ref _stopCalls);
    }

    private sealed class ProbeDependency : IDisposable
    {
        private readonly StartProbe _probe;

        public ProbeDependency(StartProbe probe)
        {
            _probe = probe;
        }

        public void Dispose()
        {
            _probe.DependencyDisposed = true;
            if (_probe.ThrowOnDependencyDispose)
                Fail();
        }

        private static void Fail() => throw new InvalidOperationException("Probe dependency dispose failure.");
    }

    private sealed class RunningService : IHostedService
    {
        private readonly StartProbe _probe;

        public RunningService(StartProbe probe)
        {
            _probe = probe;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _probe.RecordStop();
            return Task.CompletedTask;
        }
    }

    private sealed class FailingService : IHostedService
    {
        private readonly Exception _failure;

        public FailingService(Exception failure)
        {
            _failure = failure;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.FromException(_failure);

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }
}
