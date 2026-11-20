using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.TestKit;
using Squirix.Server.TestKit.Logging;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Verifies that a failed or cancelled server startup stops and releases the application and surfaces the original failure.</summary>
public sealed class ServerStartFailureTests
{
    private const int CleanupFailedEventId = 3018;

    /// <summary>A hosted service that fails to start leaves no running service and the original exception reaches the caller.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedStartStopsStartedServices(CancellationToken cancellationToken)
    {
        var probe = new StartProbe();
        var failure = new InvalidOperationException("Probe start failure.");
        var app = BuildApp(probe, failure);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartAppAsync(app, static (_, _) => Task.CompletedTask, CancellationToken.None), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    /// <summary>A cancelled start disposes the application and surfaces the cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelledStartDisposesApplication(CancellationToken cancellationToken)
    {
        var probe = new StartProbe();
        var app = BuildApp(probe, null);
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(StartAppAsync(app, static (_, _) => Task.CompletedTask, cts.Token), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(IsContainerDisposed(app)).IsTrue();
    }

    /// <summary>A start cancelled after a hosted service started stops that service, disposes its dependency and surfaces the cancellation.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CancelAfterStartedServiceStopsIt(CancellationToken cancellationToken)
    {
        using var cts = new CancellationTokenSource();
        var probe = new StartProbe { CancelOnStart = cts };
        var app = BuildApp(probe, null, null, false, true);

        _ = await NodeAsyncAssert.ThrowsAnyAsync<OperationCanceledException>(StartAppAsync(app, static (_, _) => Task.CompletedTask, cts.Token), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(probe.StopCalls).IsEqualTo(1);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
        _ = await Assert.That(IsContainerDisposed(app)).IsTrue();
    }

    /// <summary>A failure before startup begins disposes the built application and rethrows the original exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task FailedConfigureDisposesApplication(CancellationToken cancellationToken)
    {
        var probe = new StartProbe();
        var failure = new InvalidOperationException("Configure failure.");
        var app = BuildApp(probe, null);

        var start = StartAppAsync(app, (_, _) => throw failure, CancellationToken.None);
        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(start, TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(IsContainerDisposed(app)).IsTrue();
    }

    /// <summary>A cleanup failure is logged and never replaces the original startup exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task CleanupFailureDoesNotReplaceStartFailure(CancellationToken cancellationToken)
    {
        var probe = new StartProbe { ThrowOnDependencyDispose = true };
        var failure = new InvalidOperationException("Probe start failure.");
        var log = new EventRecordingLogger();
        var app = BuildApp(probe, failure, log);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartAppAsync(app, static (_, _) => Task.CompletedTask, CancellationToken.None), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        var entry = log.Find(CleanupFailedEventId);

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
        _ = await Assert.That(entry?.Level).IsEqualTo(LogLevel.Error);
        _ = await Assert.That(entry?.Cause).IsTypeOf<InvalidOperationException>();
    }

    /// <summary>A logger that throws while recording the cleanup failure does not replace the original startup exception.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task ThrowingLoggerDoesNotReplaceStartFailure(CancellationToken cancellationToken)
    {
        var probe = new StartProbe { ThrowOnDependencyDispose = true };
        var failure = new InvalidOperationException("Probe start failure.");
        var app = BuildApp(probe, failure, null, true);

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException>(StartAppAsync(app, static (_, _) => Task.CompletedTask, CancellationToken.None), TimeSpan.FromSeconds(30), TimeProvider.System, cancellationToken);

        _ = await Assert.That(thrown).IsSameReferenceAs(failure);
        _ = await Assert.That(probe.DependencyDisposed).IsTrue();
    }

    private static Task<SquirixServer.ApplicationHandle> StartAppAsync(ServerApplication app, Func<ServerApplication, CancellationToken, Task> configure, CancellationToken cancellationToken) =>
        SquirixServer.ApplicationHandle.StartApplicationAsync(app, configure, cancellationToken).AsTask();

    private static bool IsContainerDisposed(ServerApplication app)
    {
        try
        {
            _ = app.Services.Find<StartProbe>();
            return false;
        }
        catch (ObjectDisposedException)
        {
            return true;
        }
    }

    private static ServerApplication BuildApp(StartProbe probe, Exception? startFailure, EventRecordingLogger? log = null, bool throwingLogger = false, bool observeCancellation = false)
    {
        var services = new NodeServices();
        if (throwingLogger)
            _ = services.Own<ILoggerFactory>(static _ => new FixedLoggerFactory(new ThrowingLogger()));
        else
            _ = services.Own<ILoggerFactory>(_ => log == null ? NullLoggerFactory.Instance : new FixedLoggerFactory(log));

        _ = services.Use(probe);
        _ = services.Own(static serviceProvider => new ProbeDependency(serviceProvider.Require<StartProbe>()));
        _ = services.Own<INodeService>(serviceProvider =>
        {
            _ = serviceProvider.Require<ProbeDependency>();
            return new RunningService(probe);
        });
        if (startFailure != null)
            _ = services.Own<INodeService>(_ => new FailingService(startFailure));

        if (observeCancellation)
            _ = services.Own<INodeService>(static _ => new CancellationObservingService());

        return ServerApplication.Build(services);
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

        internal CancellationTokenSource? CancelOnStart { get; init; }

        internal void RecordStop() => _ = Interlocked.Increment(ref _stopCalls);
    }

    private sealed class ProbeDependency : IDisposable
    {
        private readonly StartProbe _probe;

        internal ProbeDependency(StartProbe probe)
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

    private sealed class RunningService : INodeService
    {
        private readonly StartProbe _probe;

        internal RunningService(StartProbe probe)
        {
            _probe = probe;
        }

        public Task StartAsync(CancellationToken cancellationToken) => _probe.CancelOnStart?.CancelAsync() ?? Task.CompletedTask;

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _probe.RecordStop();
            return Task.CompletedTask;
        }
    }

    private sealed class FailingService : INodeService
    {
        private readonly Exception _failure;

        internal FailingService(Exception failure)
        {
            _failure = failure;
        }

        public Task StartAsync(CancellationToken cancellationToken) => Task.FromException(_failure);

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class CancellationObservingService : INodeService
    {
        public Task StartAsync(CancellationToken cancellationToken)
        {
            cancellationToken.ThrowIfCancellationRequested();
            return Task.CompletedTask;
        }

        public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
    }

    private sealed class ThrowingLogger : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id == CleanupFailedEventId)
                throw new InvalidOperationException("Logger failure.");
        }
    }
}
