using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Squirix.Server.TestKit;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that disposing the server application handle stops the host before disposing it.</summary>
public sealed class ServerDisposeTests
{
    private const int HostStopFailedEventId = 3017;

    /// <summary>Hosted services stop while their dependencies are still alive.</summary>
    [Test]
    public async Task DisposeStopsHostBeforeContainerDispose()
    {
        var probe = new ShutdownProbe();
        var handle = await StartHandleAsync(probe, null);

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
        var handle = await StartHandleAsync(probe, null);

        var first = DisposeAsTaskAsync(handle);
        var second = DisposeAsTaskAsync(handle);
        var third = DisposeAsTaskAsync(handle);
        await Task.WhenAll(first, second, third);
        await DisposeAsTaskAsync(handle);

        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
    }

    /// <summary>A failing hosted service stop is logged as an error and the host is still disposed.</summary>
    [Test]
    public async Task DisposeLogsStopFailureAndDisposesHost()
    {
        var probe = new ShutdownProbe { ThrowOnStop = true };
        var log = new EventRecordingLogger();
        var handle = await StartHandleAsync(probe, log);

        await DisposeAsTaskAsync(handle);

        var entry = log.Find(HostStopFailedEventId);

        _ = await Assert.That(entry?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(entry?.Cause).IsNotNull();
        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposedAtStop).IsFalse();
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    /// <summary>Later and concurrent callers wait for the first disposal to finish.</summary>
    [Test]
    public async Task LaterCallersWaitForFirstDisposal()
    {
        var probe = new ShutdownProbe { StopGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously) };
        var handle = await StartHandleAsync(probe, null);

        var first = DisposeAsTaskAsync(handle);
        await probe.WaitForStopAsync();
        var second = DisposeAsTaskAsync(handle);
        var third = DisposeAsTaskAsync(handle);

        _ = await Assert.That(first.IsCompleted).IsFalse();
        _ = await Assert.That(second.IsCompleted).IsFalse();
        _ = await Assert.That(third.IsCompleted).IsFalse();

        probe.StopGate.SetResult();
        await Task.WhenAll(first, second, third);

        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
    }

    /// <summary>A container disposal failure reaches the first and every later caller.</summary>
    [Test]
    public async Task ContainerDisposeFailureReachesAllCallers()
    {
        var probe = new ShutdownProbe { ThrowOnDependencyDispose = true };
        var handle = await StartHandleAsync(probe, null);

        var first = DisposeAsTaskAsync(handle);
        var second = DisposeAsTaskAsync(handle);

        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(first);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(second);
        _ = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(DisposeAsTaskAsync(handle));
    }

    private static Task DisposeAsTaskAsync(SquirixServer.ApplicationHandle handle) => handle.DisposeAsync().AsTask();

    private static async Task<SquirixServer.ApplicationHandle> StartHandleAsync(ShutdownProbe probe, EventRecordingLogger? log)
    {
        var builder = WebApplication.CreateSlimBuilder(new WebApplicationOptions { Args = [] });
        _ = builder.WebHost.UseUrls("http://127.0.0.1:0");
        _ = builder.Logging.ClearProviders();
        if (log != null)
            _ = builder.Services.AddSingleton<ILoggerProvider>(_ => new RecordingLoggerProvider(log));

        _ = builder.Services.AddSingleton(_ => new ProbeDependency(probe));
        _ = builder.Services.AddSingleton<IHostedService>(serviceProvider => new ProbeHostedService(probe, serviceProvider.GetRequiredService<ProbeDependency>()));
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

        internal bool ThrowOnStop { get; init; }

        internal bool ThrowOnDependencyDispose { get; init; }

        internal TaskCompletionSource? StopGate { get; init; }

        private TaskCompletionSource StopEntered { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

#pragma warning disable VSTHRD003 // The task is a completion source owned and signalled by this probe.
        internal Task WaitForStopAsync() => StopEntered.Task;
#pragma warning restore VSTHRD003

        internal void RecordStop(bool dependencyDisposed)
        {
            _dependencyDisposedAtStop = dependencyDisposed;
            _ = Interlocked.Increment(ref _stopCalls);
            _ = StopEntered.TrySetResult();
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

        public void Dispose()
        {
            _probe.DependencyDisposed = true;
            if (_probe.ThrowOnDependencyDispose)
                Fail();
        }

        private static void Fail() => throw new InvalidOperationException("Probe dependency dispose failure.");
    }

    private sealed class ProbeHostedService : IHostedService
    {
        private readonly ProbeDependency _dependency;
        private readonly ShutdownProbe _probe;

        public ProbeHostedService(ShutdownProbe probe, ProbeDependency dependency)
        {
            _probe = probe;
            _dependency = dependency;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

        public async Task StopAsync(CancellationToken cancellationToken)
        {
            _probe.RecordStop(_dependency.IsDisposed);
            if (_probe.StopGate != null)
                await _probe.StopGate.Task.WaitAsync(cancellationToken).ConfigureAwait(false);

            if (_probe.ThrowOnStop)
                throw new InvalidOperationException("Probe stop failure.");
        }
    }
}
