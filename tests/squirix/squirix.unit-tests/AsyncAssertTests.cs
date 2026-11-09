using System;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Sources;
using Microsoft.Extensions.Time.Testing;
using Squirix.TestKit;
using TUnit.Assertions;
using TUnit.Assertions.Exceptions;
using TUnit.Assertions.Extensions;
using TUnit.Core;

namespace Squirix.UnitTests;

/// <summary>Verifies that assertion guards cannot become expected operation failures.</summary>
public sealed class AsyncAssertTests
{
    /// <summary>A pending operation must not turn the guard timeout into a successful assertion.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GuardTimeoutCannotPass(bool derived)
    {
        var clock = new FakeTimeProvider();
        var pending = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        var assertion = ProbeAsync<TimeoutException>(pending.Task, derived, TimeSpan.FromSeconds(1), clock, CancellationToken.None);
        try
        {
            clock.Advance(TimeSpan.FromMilliseconds(999));
            _ = await Assert.That(assertion.IsCompleted).IsFalse();
            clock.Advance(TimeSpan.FromMilliseconds(1));
            _ = await Assert.That(assertion.IsFaulted).IsTrue();
            _ = await Assert.That(assertion.Exception!.InnerException).IsTypeOf<TimeoutException>();
            _ = await Assert.That(pending.Task.IsCompleted).IsFalse();
        }
        finally
        {
            pending.SetResult(0);
        }
    }

    /// <summary>A timeout produced by the operation itself remains an accepted exception.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OperationTimeoutIsPreserved(bool derived)
    {
        var failure = new TimeoutException("operation");
        var actual = await ProbeAsync<TimeoutException>(Task.FromException<int>(failure), derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(actual).IsSameReferenceAs(failure);
    }

    /// <summary>Cancellation of the assertion wait is not an operation failure.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task GuardCancellationCannotPass(bool derived)
    {
        var pending = new TaskCompletionSource<int>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var cancellation = new CancellationTokenSource();
        var assertion = ProbeAsync<OperationCanceledException>(pending.Task, derived, TimeSpan.FromSeconds(1), new FakeTimeProvider(), cancellation.Token);
        try
        {
            await cancellation.CancelAsync();
            _ = await Assert.That(assertion.IsCanceled).IsTrue();
            _ = await Assert.That(pending.Task.IsCompleted).IsFalse();
        }
        finally
        {
            pending.SetResult(0);
        }
    }

    /// <summary>Exception matching preserves exact and derived-type semantics.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task DerivedMatchingIsPreserved(bool derived)
    {
        var failure = new ArgumentNullException(nameof(derived));
        var assertion = ProbeAsync<ArgumentException>(Task.FromException<int>(failure), derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        if (derived)
        {
            _ = await Assert.That(await assertion).IsSameReferenceAs(failure);
        }
        else
        {
            _ = await Assert.That(assertion.IsFaulted).IsTrue();
            _ = await Assert.That(assertion.Exception!.InnerException).IsSameReferenceAs(failure);
        }
    }

    /// <summary>Successful completion cannot satisfy an exception assertion.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task SuccessfulOperationFails(bool derived)
    {
        var assertion = ProbeAsync<InvalidOperationException>(Task.FromResult(42), derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(assertion.IsFaulted).IsTrue();
        _ = await Assert.That(assertion.Exception!.InnerException).IsTypeOf<AssertionException>();
    }

    /// <summary>Unrelated operation errors propagate without becoming assertion results.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task WrongExceptionIsPreserved(bool derived)
    {
        var failure = new InvalidOperationException("operation");
        var assertion = ProbeAsync<TimeoutException>(Task.FromException<int>(failure), derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(assertion.IsFaulted).IsTrue();
        _ = await Assert.That(assertion.Exception!.InnerException).IsSameReferenceAs(failure);
    }

    /// <summary>Cancellation produced by the operation remains an expected exception.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task OperationCancellationIsPreserved(bool derived)
    {
        var failure = new OperationCanceledException("operation");
        var actual = await ProbeAsync<OperationCanceledException>(Task.FromException<int>(failure), derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(actual).IsSameReferenceAs(failure);
    }

    /// <summary>Unbounded and unsupported guard durations are rejected before the operation is consumed.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task InvalidTimeoutIsRejected(bool derived)
    {
        foreach (var timeout in new[] { Timeout.InfiniteTimeSpan, TimeSpan.FromMilliseconds(-2), TimeSpan.FromMilliseconds(uint.MaxValue) })
        {
            ArgumentOutOfRangeException? failure = null;
            try
            {
                _ = ProbeAsync<InvalidOperationException>(Task.FromResult(1), derived, timeout, TimeProvider.System, CancellationToken.None);
            }
            catch (ArgumentOutOfRangeException error)
            {
                failure = error;
            }

            _ = await Assert.That(failure).IsNotNull();
        }
    }

    /// <summary>An already canceled task still supplies its own cancellation exception.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task CanceledTaskIsAccepted(bool derived)
    {
        using var cancellation = new CancellationTokenSource();
        await cancellation.CancelAsync();
        var operation = Task.FromCanceled<int>(cancellation.Token);
        var failure = await ProbeAsync<TaskCanceledException>(operation, derived, TimeSpan.Zero, TimeProvider.System, CancellationToken.None);
        _ = await Assert.That(failure.CancellationToken).IsEqualTo(cancellation.Token);
    }

    /// <summary>ValueTask sources are consumed once while preserving the original exception.</summary>
    /// <param name="derived">Whether derived exception types are accepted.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task ValueTaskIsConsumedOnce(bool derived, CancellationToken cancellationToken)
    {
        var failure = new InvalidOperationException("operation");
        var source = new SingleUseSource(failure);
        var operation = new ValueTask<int>(source, source.Version);
        var actual = derived
            ? await AsyncAssert.ThrowsAnyAsync<InvalidOperationException, int>(operation, TimeSpan.Zero, TimeProvider.System, cancellationToken)
            : await AsyncAssert.ThrowsAsync<InvalidOperationException, int>(operation, TimeSpan.Zero, TimeProvider.System, cancellationToken);
        _ = await Assert.That(actual).IsSameReferenceAs(failure);
        _ = await Assert.That(source.Reads).IsEqualTo(1);
    }

    /// <summary>The root type is rejected synchronously for both matching modes.</summary>
    /// <param name="derived">Whether derived exception types would otherwise be accepted.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RootTypeIsRejected(bool derived, CancellationToken cancellationToken)
    {
        var failure = ExceptionAssert.For<AssertionException>().Throws(
            (Derived: derived, Token: cancellationToken),
            static state => _ = ProbeAsync<Exception>(Task.FromResult(1), state.Derived, TimeSpan.Zero, TimeProvider.System, state.Token));
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }

    /// <summary>An invalid expected type fails before a single-use ValueTask source is consumed.</summary>
    /// <param name="derived">Whether derived exception types would otherwise be accepted.</param>
    /// <param name="cancellationToken">The test cancellation token.</param>
    [Test]
    [Arguments(false)]
    [Arguments(true)]
    public async Task RootValueTaskIsNotConsumed(bool derived, CancellationToken cancellationToken)
    {
        var source = new SingleUseSource(new InvalidOperationException("operation"));
        var failure = ExceptionAssert.For<AssertionException>().Throws(
            (Operation: new ValueTask<int>(source, source.Version), Derived: derived, Token: cancellationToken),
            static state =>
            {
                _ = state.Derived
                    ? AsyncAssert.ThrowsAnyAsync<Exception, int>(state.Operation, TimeSpan.Zero, TimeProvider.System, state.Token)
                    : AsyncAssert.ThrowsAsync<Exception, int>(state.Operation, TimeSpan.Zero, TimeProvider.System, state.Token);
            });
        _ = await Assert.That(failure.Message).IsEqualTo("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
        _ = await Assert.That(source.Reads).IsEqualTo(0);
    }

    private static Task<TException> ProbeAsync<TException>(Task<int> operation, bool derived, TimeSpan timeout, TimeProvider clock, CancellationToken cancellationToken)
        where TException : Exception
    {
        return derived
            ? AsyncAssert.ThrowsAnyAsync<TException, int>(new ValueTask<int>(operation), timeout, clock, cancellationToken)
            : AsyncAssert.ThrowsAsync<TException, int>(new ValueTask<int>(operation), timeout, clock, cancellationToken);
    }

    private sealed class SingleUseSource : IValueTaskSource<int>
    {
        private ManualResetValueTaskSourceCore<int> _source;

        public SingleUseSource(Exception failure)
        {
            _source.RunContinuationsAsynchronously = true;
            _source.SetException(failure);
        }

        public short Version => _source.Version;

        public int Reads { get; private set; }

        public int GetResult(short token)
        {
            Reads++;
            return _source.GetResult(token);
        }

        public ValueTaskSourceStatus GetStatus(short token) => _source.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            _source.OnCompleted(continuation, state, token, flags);
    }
}
