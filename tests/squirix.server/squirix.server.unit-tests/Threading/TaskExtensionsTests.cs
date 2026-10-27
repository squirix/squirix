using System;
using System.IO;
using System.Threading.Tasks;
using Squirix.Server.Attributes;
using Squirix.Server.TestKit;
using Squirix.Server.Threading;
using TUnit.Assertions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests.Threading;

/// <summary>A captured failure is returned to the caller; a failure the filter rejects still propagates.</summary>
[Immutable]
public sealed class TaskExtensionsTests
{
    /// <summary>A successful task yields no failure.</summary>
    [Test]
    public async Task CaptureFailureReturnsNullOnSuccess()
    {
        var failure = await Task.CompletedTask.CaptureFailureAsync(static _ => true);

        _ = await Assert.That(failure).IsNull();
    }

    /// <summary>A failure the filter accepts is returned instead of thrown.</summary>
    [Test]
    public async Task CaptureFailureReturnsAcceptedFailure()
    {
        var expected = new IOException("disk failed");

        var failure = await Task.FromException(expected).CaptureFailureAsync(static ex => ex is IOException);

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    /// <summary>A failure the filter rejects propagates to the caller.</summary>
    [Test]
    public async Task CaptureFailureRethrowsRejectedFailure()
    {
        var expected = new InvalidOperationException("unexpected");

        var thrown = await NodeAsyncAssert.ThrowsAsync<InvalidOperationException, Exception?>(
            Task.FromException(expected).CaptureFailureAsync(static ex => ex is IOException));

        _ = await Assert.That(thrown).IsSameReferenceAs(expected);
    }

    /// <summary>A successful task yields no failure without a filter.</summary>
    [Test]
    public async Task CaptureUnfilteredReturnsNullOnSuccess()
    {
        var failure = await Task.CompletedTask.CaptureFailureAsync();

        _ = await Assert.That(failure).IsNull();
    }

    /// <summary>A task that faults after being awaited yields the exception its body threw.</summary>
    [Test]
    public async Task CaptureUnfilteredReturnsThrownInstance()
    {
        var expected = new IOException("late failure");

        var failure = await ThrowLaterAsync(expected).CaptureFailureAsync();

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    /// <summary>A canceled task yields a <see cref="TaskCanceledException" /> that references it.</summary>
    [Test]
    public async Task CaptureUnfilteredReturnsTaskCanceled()
    {
        using var source = new System.Threading.CancellationTokenSource();
        await source.CancelAsync();
        var canceled = Task.FromCanceled(source.Token);

        var failure = await canceled.CaptureFailureAsync();

        _ = await Assert.That(failure is TaskCanceledException { Task: { } owner } && ReferenceEquals(owner, canceled)).IsTrue();
    }

    /// <summary>A canceled task is offered to the filter as a <see cref="TaskCanceledException" />.</summary>
    [Test]
    public async Task CaptureFilteredReturnsTaskCanceled()
    {
        using var source = new System.Threading.CancellationTokenSource();
        await source.CancelAsync();

        var failure = await Task.FromCanceled(source.Token).CaptureFailureAsync(static ex => ex is OperationCanceledException);

        _ = await Assert.That(failure).IsTypeOf<TaskCanceledException>();
    }

    /// <summary>A pending task that later faults yields its exception through the filter.</summary>
    [Test]
    public async Task CaptureFilteredReturnsLateFailure()
    {
        var expected = new IOException("late failure");

        var failure = await ThrowLaterAsync(expected).CaptureFailureAsync(static ex => ex is IOException);

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    /// <summary>A failed value task yields the accepted failure the same way.</summary>
    [Test]
    public async Task CaptureValueTaskFailure()
    {
        var expected = new TimeoutException("stop timed out");

        var failure = await ValueTask.FromException(expected).CaptureFailureAsync(static ex => ex is TimeoutException);

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    /// <summary>A disposal that faults is returned instead of thrown.</summary>
    [Test]
    public async Task CaptureDisposeReturnsFault()
    {
        var expected = new InvalidOperationException("dispose faulted");

        var failure = await Failing(expected, false).CaptureFailureAsync();

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    /// <summary>A disposal that throws before returning its task is returned instead of thrown.</summary>
    [Test]
    public async Task CaptureDisposeReturnsSyncThrow()
    {
        var expected = new InvalidOperationException("dispose threw");

        var failure = await Failing(expected, true).CaptureFailureAsync();

        _ = await Assert.That(failure).IsSameReferenceAs(expected);
    }

    private static async Task ThrowLaterAsync(Exception failure)
    {
        await Task.Yield();
        throw failure;
    }

    private static FailingDisposable Failing(Exception failure, bool throwDirectly) => new(failure, throwDirectly);

    /// <summary>Fails its disposal either by throwing directly or by returning a faulted task.</summary>
    private sealed class FailingDisposable : IAsyncDisposable
    {
        private readonly Exception _failure;
        private readonly bool _throwDirectly;

        internal FailingDisposable(Exception failure, bool throwDirectly)
        {
            _failure = failure;
            _throwDirectly = throwDirectly;
        }

        public ValueTask DisposeAsync() => _throwDirectly ? throw _failure : ValueTask.FromException(_failure);
    }
}
