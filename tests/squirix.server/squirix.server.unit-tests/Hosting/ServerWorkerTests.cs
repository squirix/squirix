using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Server.Composition;
using Squirix.Server.Logging;
using Squirix.Server.Node.Hosting;
using Squirix.Server.UnitTests.Support;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Hosting;

/// <summary>Exercises supervision of a running node service.</summary>
public sealed class ServerWorkerTests
{
    /// <summary>A worker failure logs its original cause and requests shutdown, including cancellation the node did not request.</summary>
    /// <param name="unexpectedCancellation">Whether the worker reports unexpected cancellation.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WorkerFailureRequestsShutdown(bool unexpectedCancellation, CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Exception failure = unexpectedCancellation ? new OperationCanceledException("worker cancelled") : new InvalidOperationException("worker failed");
        var log = new EventRecordingLogger();
        var components = new NodeServices();
        _ = components.Own<IServerLoggerFactory>(_ => new FixedLoggerFactory(log));
        _ = components.RunService(_ => new FaultingWorker(release.Task, failure));
        await using var app = ServerApplication.Build(components);
        await app.StartAsync(cancellationToken);
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var registration = app.Services.Require<INodeLifetime>().ApplicationStopping.Register(stopped.SetResult);

        release.SetResult();
        await stopped.Task.WaitAsync(TimeSpan.FromSeconds(10), TimeProvider.System, cancellationToken);
        await app.StopAsync(cancellationToken);

        _ = await Assert.That(log.Find(3090)?.Cause).IsSameReferenceAs(failure);
    }

    private sealed class FaultingWorker : NodeWorker
    {
        private readonly Task _release;
        private readonly Exception _failure;

        internal FaultingWorker(Task release, Exception failure)
        {
            _release = release;
            _failure = failure;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            await _release.WaitAsync(stoppingToken).ConfigureAwait(false);
            throw _failure;
        }
    }
}
