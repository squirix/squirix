using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Time.Testing;
using Squirix.Server.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.Server.UnitTests;

/// <summary>Verifies explicitly listed alternative exception types without accepting arbitrary failures.</summary>
public sealed class NodeExceptionSetTests
{
    /// <summary>Either listed type returns the original exception object.</summary>
    /// <param name="first">Whether the operation throws the first listed type.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OneOfReturnsListedFailure(bool first, CancellationToken cancellationToken)
    {
        Exception expected = first ? new TimeoutException("operation") : new InvalidOperationException("operation");
        var actual = await NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(Task.FromException(expected), TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(actual).IsSameReferenceAs(expected);
    }

    /// <summary>An unlisted failure is propagated unchanged.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsUnexpectedType(CancellationToken cancellationToken)
    {
        var expected = new IOException("operation");
        var assertion = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(Task.FromException(expected), TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(assertion.IsFaulted).IsTrue();
        _ = await Assert.That(assertion.Exception!.InnerException).IsSameReferenceAs(expected);
    }

    /// <summary>A listed subtype is accepted even when its base type appears first in the allowed set.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfAcceptsListedDerivedType(CancellationToken cancellationToken)
    {
        var expected = new ArgumentNullException(nameof(cancellationToken));
        var actual = await NodeAsyncAssert.ThrowsOneOfAsync<ArgumentException, ArgumentNullException>(Task.FromException(expected), TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(actual).IsSameReferenceAs(expected);
    }

    /// <summary>A subclass of a listed type is not an exact match.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsDerivedType(CancellationToken cancellationToken)
    {
        var expected = new ArgumentNullException(nameof(cancellationToken));
        var assertion = NodeAsyncAssert.ThrowsOneOfAsync<ArgumentException, TimeoutException>(Task.FromException(expected), TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(assertion.IsFaulted).IsTrue();
        _ = await Assert.That(assertion.Exception!.InnerException).IsSameReferenceAs(expected);
    }

    /// <summary>Successful completion cannot satisfy an alternative exception assertion.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsSuccessfulOperation(CancellationToken cancellationToken)
    {
        var assertion = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(Task.CompletedTask, TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(assertion.IsFaulted).IsTrue();
        _ = await Assert.That(assertion.Exception!.InnerException).IsTypeOf<AssertionException>();
    }

    /// <summary>The root type is forbidden in either generic argument before the operation is observed.</summary>
    /// <param name="first">Whether the root type is in the first slot.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OneOfRejectsRootInEitherSlot(bool first, CancellationToken cancellationToken)
    {
        var failure = NodeExceptionAssert.For<AssertionException>().Throws(
            (First: first, Token: cancellationToken),
            static state =>
            {
                _ = state.First
                    ? NodeAsyncAssert.ThrowsOneOfAsync<Exception, TimeoutException>(Task.CompletedTask, TimeSpan.Zero, TimeProvider.System, state.Token)
                    : NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, Exception>(Task.CompletedTask, TimeSpan.Zero, TimeProvider.System, state.Token);
            });
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }

    /// <summary>A guard timeout must not satisfy either expected type.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfGuardTimeoutCannotPass(CancellationToken cancellationToken)
    {
        var clock = new FakeTimeProvider();
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var assertion = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(pending.Task, TimeSpan.FromSeconds(1), clock, cancellationToken);
        try
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            _ = await Assert.That(assertion.IsFaulted).IsTrue();
            _ = await Assert.That(assertion.Exception!.InnerException).IsTypeOf<TimeoutException>();
            _ = await Assert.That(pending.Task.IsCompleted).IsFalse();
        }
        finally
        {
            pending.SetResult();
        }
    }

    /// <summary>Cancellation of the guard must not satisfy either expected type.</summary>
    [Test]
    public async Task OneOfGuardCancellationCannotPass()
    {
        using var cancellation = new CancellationTokenSource();
        var pending = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var assertion = NodeAsyncAssert.ThrowsOneOfAsync<OperationCanceledException, TimeoutException>(pending.Task, TimeSpan.FromSeconds(1), new FakeTimeProvider(), cancellation.Token);
        try
        {
            await cancellation.CancelAsync();
            _ = await Assert.That(assertion.IsCanceled).IsTrue();
            _ = await Assert.That(pending.Task.IsCompleted).IsFalse();
        }
        finally
        {
            pending.SetResult();
        }
    }

    /// <summary>Alternative exception matching cannot disable the finite guard requirement.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsInfiniteTimeout(CancellationToken cancellationToken)
    {
        var failure = NodeExceptionAssert.For<ArgumentOutOfRangeException>().Throws(
            cancellationToken,
            static token => _ = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(Task.CompletedTask, Timeout.InfiniteTimeSpan, TimeProvider.System, token));
        _ = await Assert.That(failure.ParamName).IsEqualTo("timeout");
    }

    /// <summary>A null operation is rejected synchronously.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsNullOperation(CancellationToken cancellationToken)
    {
        const Task? missingOperation = null;
        var failure = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            cancellationToken,
            static token => _ = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(missingOperation!, TimeSpan.Zero, TimeProvider.System, token));
        _ = await Assert.That(failure.ParamName).IsEqualTo("operation");
    }

    /// <summary>A null clock is rejected synchronously.</summary>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    public async Task OneOfRejectsNullClock(CancellationToken cancellationToken)
    {
        const TimeProvider? missingClock = null;
        var failure = NodeExceptionAssert.For<ArgumentNullException>().Throws(
            cancellationToken,
            static token => _ = NodeAsyncAssert.ThrowsOneOfAsync<TimeoutException, InvalidOperationException>(Task.CompletedTask, TimeSpan.Zero, missingClock!, token));
        _ = await Assert.That(failure.ParamName).IsEqualTo("timeProvider");
    }
}
