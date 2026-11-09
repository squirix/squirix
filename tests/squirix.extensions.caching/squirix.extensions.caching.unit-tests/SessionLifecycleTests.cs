using System;
using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Extensions.Caching.UnitTests;

/// <summary>Exercises the provider's connection and shutdown coordination at controlled boundaries.</summary>
public sealed class SessionLifecycleTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>Concurrent waiters share a connection, and a cancelled waiter leaves it running.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Reliability", "CA2000", Justification = "The session owns and disposes the connection returned by the connector.")]
    public async Task CancelledWaiterLeavesConnectionRunning(CancellationToken cancellationToken)
    {
        var attempt = new TaskCompletionSource<Connection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var calls = 0;
        Task<Connection> ConnectAsync(CancellationToken token)
        {
            _ = Interlocked.Increment(ref calls);
            return attempt.Task.WaitAsync(token);
        }

        await using var session = new DistributedCacheSession<Connection>(ConnectAsync, TimeProvider.System);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        var first = session.GetAsync(cancelled.Token);
        var second = session.GetAsync(cancellationToken);
        var third = session.GetAsync(cancellationToken);
        await cancelled.CancelAsync();
        var error = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, Connection>(new ValueTask<Connection>(first), Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(error.CancellationToken).IsEqualTo(cancelled.Token);
        _ = await Assert.That(session.LifetimeToken.IsCancellationRequested).IsFalse();
        _ = await Assert.That(second.IsCompleted).IsFalse();
        _ = await Assert.That(third.IsCompleted).IsFalse();

        var opened = new Connection();
        attempt.SetResult(opened);
        _ = await Assert.That(ReferenceEquals(await second, opened)).IsTrue();
        _ = await Assert.That(ReferenceEquals(await third, opened)).IsTrue();
        _ = await Assert.That(Volatile.Read(ref calls)).IsEqualTo(1);
        await session.DisposeAsync();
        _ = await Assert.That(opened.DisposalCalls).IsEqualTo(1);
    }

    /// <summary>A failed attempt is shared, then the next caller opens a new connection.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Reliability", "CA2000", Justification = "The session owns and disposes the connection returned by the connector.")]
    public async Task FailedConnectionCanBeRetried(CancellationToken cancellationToken)
    {
        var firstAttempt = new TaskCompletionSource<Connection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var opened = new Connection();
        var calls = 0;
        Task<Connection> ConnectAsync(CancellationToken token)
        {
            return Interlocked.Increment(ref calls) == 1 ? firstAttempt.Task.WaitAsync(token) : Task.FromResult(opened);
        }

        await using var session = new DistributedCacheSession<Connection>(ConnectAsync, TimeProvider.System);
        var first = session.GetAsync(cancellationToken);
        var second = session.GetAsync(cancellationToken);
        var failure = new InvalidOperationException("connection failed");
        firstAttempt.SetException(failure);
        var observedFirst = await AsyncAssert.ThrowsAsync<InvalidOperationException, Connection>(new ValueTask<Connection>(first), Bound, TimeProvider.System, cancellationToken);
        var observedSecond = await AsyncAssert.ThrowsAsync<InvalidOperationException, Connection>(new ValueTask<Connection>(second), Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(ReferenceEquals(observedFirst, failure)).IsTrue();
        _ = await Assert.That(ReferenceEquals(observedSecond, failure)).IsTrue();
        _ = await Assert.That(ReferenceEquals(await session.GetAsync(cancellationToken), opened)).IsTrue();
        _ = await Assert.That(Volatile.Read(ref calls)).IsEqualTo(2);
    }

    /// <summary>Shutdown returns at 30 seconds and still releases a connection that arrives later.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Reliability", "CA2000", Justification = "The test verifies that the session disposes the late connection.")]
    public async Task TimeoutStillReleasesLateConnection(CancellationToken cancellationToken)
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempt = new TaskCompletionSource<Connection>(TaskCreationOptions.RunContinuationsAsynchronously);
        var clock = new FakeTimeProvider();
        var session = new DistributedCacheSession<Connection>(
            _ =>
            {
                started.SetResult();
                return attempt.Task;
            },
            clock);
        var waiting = session.GetAsync(session.LifetimeToken);
        await started.Task.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        var shutdown = BeginShutdownAsync(session);
        var repeated = BeginShutdownAsync(session);
        _ = await Assert.That(ReferenceEquals(shutdown, repeated)).IsTrue();
        _ = await Assert.That(session.LifetimeToken.IsCancellationRequested).IsTrue();
        _ = ExceptionAssert.For<ObjectDisposedException>().Throws(session, static disposed => disposed.ThrowIfDisposed());
        _ = await AsyncAssert.ThrowsAnyAsync<OperationCanceledException, Connection>(new ValueTask<Connection>(waiting), Bound, TimeProvider.System, cancellationToken);
        clock.Advance(TimeSpan.FromSeconds(29));
        _ = await Assert.That(shutdown.IsCompleted).IsFalse();
        clock.Advance(TimeSpan.FromSeconds(1));
        _ = await AsyncAssert.ThrowsAsync<TimeoutException, bool>(AwaitCompletionAsync(shutdown), Bound, TimeProvider.System, cancellationToken);

        var opened = new Connection();
        attempt.SetResult(opened);
        await opened.Disposed.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(opened.DisposalCalls).IsEqualTo(1);
    }

    /// <summary>Shutdown cancels active work and continues to drain an open connection after timeout.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Reliability", "CA2000", Justification = "The test verifies that the session owns and drains the connection.")]
    public async Task TimeoutLeavesOneDrainRunning(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var opened = new Connection(release.Task);
        var clock = new FakeTimeProvider();
        var session = new DistributedCacheSession<Connection>(_ => Task.FromResult(opened), clock);
        _ = await session.GetAsync(cancellationToken);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        using var active = session.Link(caller.Token);
        var shutdown = BeginShutdownAsync(session);
        await opened.DisposalStarted.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(active is { IsCancellationRequested: true }).IsTrue();
        clock.Advance(TimeSpan.FromSeconds(30));
        _ = await AsyncAssert.ThrowsAsync<TimeoutException, bool>(AwaitCompletionAsync(shutdown), Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(opened.Disposed.IsCompleted).IsFalse();

        release.SetResult();
        await opened.Disposed.WaitAsync(Bound, TimeProvider.System, cancellationToken);
        _ = await Assert.That(opened.DisposalCalls).IsEqualTo(1);
    }

    /// <summary>A completed connection respects cancellation and refuses new calls as soon as shutdown begins.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [SuppressMessage("Reliability", "CA2000", Justification = "The session owns the test connection.")]
    public async Task WarmConnectionRejectsNewCallsDuringStop(CancellationToken cancellationToken)
    {
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var opened = new Connection(release.Task);
        await using var session = new DistributedCacheSession<Connection>(_ => Task.FromResult(opened), TimeProvider.System);
        _ = await session.GetAsync(cancellationToken);
        using var cancelled = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        await cancelled.CancelAsync();
        var error = ExceptionAssert.For<OperationCanceledException>().Throws((session, cancelled.Token), static state => _ = state.session.GetAsync(state.Token));
        _ = await Assert.That(error.CancellationToken).IsEqualTo(cancelled.Token);

        var stopped = BeginShutdownAsync(session);
        try
        {
            await opened.DisposalStarted.WaitAsync(Bound, TimeProvider.System, cancellationToken);
            _ = ExceptionAssert.For<ObjectDisposedException>().Throws((session, cancellationToken), static state => _ = state.session.GetAsync(state.cancellationToken));
        }
        finally
        {
            _ = release.TrySetResult();
            await stopped;
        }
    }

    private static Task BeginShutdownAsync(DistributedCacheSession<Connection> session) => session.DisposeAsync().AsTask();

    private static async ValueTask<bool> AwaitCompletionAsync(Task task)
    {
        await task.ConfigureAwait(false);
        return true;
    }

    private sealed class Connection : IAsyncDisposable
    {
        private readonly TaskCompletionSource _disposalStarted = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly TaskCompletionSource _disposed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        private readonly Task _drain;
        private int _disposalCalls;

        internal Connection(Task? drain = null)
        {
            _drain = drain ?? Task.CompletedTask;
        }

        internal Task DisposalStarted => _disposalStarted.Task;

        internal Task Disposed => _disposed.Task;

        internal int DisposalCalls => Volatile.Read(ref _disposalCalls);

        public async ValueTask DisposeAsync()
        {
            _ = Interlocked.Increment(ref _disposalCalls);
            _ = _disposalStarted.TrySetResult();
            await _drain.ConfigureAwait(false);
            _ = _disposed.TrySetResult();
        }
    }
}
