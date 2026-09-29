using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that disposing the server application handle stops the host before disposing it.</summary>
public sealed class ServerDisposeTests
{
    /// <summary>Hosted services stop while their dependencies are still alive.</summary>
    [Test]
    public async Task DisposeStopsHostBeforeContainerDispose()
    {
        var probe = new ShutdownProbe();
        var handle = await StartHandleAsync(probe, false);

        await DisposeAsTaskAsync(handle);

        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposedAtStop).IsFalse();
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    /// <summary>Repeated and concurrent disposal stops the host exactly once and never throws.</summary>
    [Test]
    public async Task DisposeIsIdempotentAndConcurrentSafe()
    {
        var probe = new ShutdownProbe();
        var handle = await StartHandleAsync(probe, false);

        var first = DisposeAsTaskAsync(handle);
        var second = DisposeAsTaskAsync(handle);
        var third = DisposeAsTaskAsync(handle);
        await Task.WhenAll(first, second, third);
        await DisposeAsTaskAsync(handle);

        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
    }

    /// <summary>A failing hosted service stop is contained and the host is still disposed.</summary>
    [Test]
    public async Task DisposeContainsStopFailure()
    {
        var probe = new ShutdownProbe();
        var handle = await StartHandleAsync(probe, true);

        await DisposeAsTaskAsync(handle);

        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    private static Task DisposeAsTaskAsync(SquirixServer.ApplicationHandle handle) => handle.DisposeAsync().AsTask();

    private static async Task<SquirixServer.ApplicationHandle> StartHandleAsync(ShutdownProbe probe, bool throwOnStop)
    {
        var builder = WebApplication.CreateBuilder(new WebApplicationOptions { Args = [] });
        _ = builder.WebHost.UseUrls("http://127.0.0.1:0");
        _ = builder.Services.AddSingleton(probe);
        _ = builder.Services.AddSingleton(_ => new ProbeDependency(probe));
        _ = builder.Services.AddSingleton<IHostedService>(serviceProvider => new ProbeHostedService(probe, serviceProvider.GetRequiredService<ProbeDependency>(), throwOnStop));
        var app = builder.Build();
        await app.StartAsync(CancellationToken.None);
        return new SquirixServer.ApplicationHandle(app);
    }

    private sealed class ShutdownProbe
    {
        private int _stopCalls;
        private volatile bool _dependencyDisposedAtStop;
        private volatile bool _dependencyDisposed;

        internal int StopCalls => Volatile.Read(ref _stopCalls);

        internal bool DependencyDisposedAtStop => _dependencyDisposedAtStop;

        internal bool DependencyDisposed
        {
            get => _dependencyDisposed;
            set => _dependencyDisposed = value;
        }

        internal void RecordStop(bool dependencyDisposed)
        {
            _dependencyDisposedAtStop = dependencyDisposed;
            _ = Interlocked.Increment(ref _stopCalls);
        }
    }

    private sealed class ProbeDependency : IDisposable
    {
        private readonly ShutdownProbe _probe;

        public ProbeDependency(ShutdownProbe probe)
        {
            _probe = probe;
        }

        internal bool IsDisposed => _probe.DependencyDisposed;

        public void Dispose() => _probe.DependencyDisposed = true;
    }

    private sealed class ProbeHostedService : IHostedService
    {
        private readonly ProbeDependency _dependency;
        private readonly ShutdownProbe _probe;
        private readonly bool _throwOnStop;

        public ProbeHostedService(ShutdownProbe probe, ProbeDependency dependency, bool throwOnStop)
        {
            _probe = probe;
            _dependency = dependency;
            _throwOnStop = throwOnStop;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _probe.RecordStop(_dependency.IsDisposed);
            return _throwOnStop ? throw new InvalidOperationException("Probe stop failure.") : Task.CompletedTask;
        }
    }
}
