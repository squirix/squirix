using System;
using System.Threading;
using System.Threading.Tasks;
using Squirix.Testing;
using TUnit.Assertions.Exceptions;

namespace Squirix.Server.TestKit;

/// <summary>Time-bounded exception assertions for asynchronous operations that are already in flight.</summary>
/// <remarks>
///     <para>
///     These helpers accept the awaitable itself instead of a delegate, so call sites do not allocate the
///     display class that <c language="csharp">Assert.ThrowsAsync</c> requires for its captured state.
///     </para>
///     <para>
///     The operation starts before the helper is entered, so only faults captured by the awaitable are
///     observed. Assertions on operations that throw synchronously (for example, argument validation in a
///     non-async method body) must keep using <c language="csharp">Assert.ThrowsAsync</c>.
///     </para>
/// </remarks>
public static class NodeAsyncAssert
{
    /// <summary>Asserts that the operation faults with exactly the expected type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="operation" /> or <paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAsync<TException>(Task operation, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException>(operation, true), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with the expected type or a derived type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="operation" /> or <paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAnyAsync<TException>(Task operation, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException>(operation, false), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with exactly the expected type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAsync<TException>(ValueTask operation, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException>(operation, true), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with the expected type or a derived type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAnyAsync<TException>(ValueTask operation, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException>(operation, false), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with exactly the expected type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <typeparam name="TResult">Operation result type, discarded after completion.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAsync<TException, TResult>(
        ValueTask<TResult> operation,
        TimeSpan timeout,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException, TResult>(operation, true), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with the expected type or a derived type.</summary>
    /// <remarks>
    /// <para>
    /// Pass the original operation. Do not add a test timeout or cancellation wrapper such as
    /// <c language="csharp">operation.WaitAsync(...)</c> before passing it here, including through an intermediate variable
    /// or a <c language="csharp">ValueTask</c> wrapper. This method observes only the supplied awaitable and cannot determine
    /// whether an exception came from the original operation or from a wrapper. A wrapper's timeout or
    /// cancellation exception can incorrectly satisfy the assertion if it matches <typeparamref name="TException" />,
    /// even while the original operation is still pending.
    /// </para>
    /// <para>
    /// Use <paramref name="timeout" /> and the test runner's <paramref name="cancellationToken" /> to bound
    /// this assertion instead. Their timeout and cancellation are outside exception matching and cannot
    /// satisfy this assertion. Neither cancels the original operation; the caller remains responsible for
    /// its cleanup. An expected timeout or cancellation produced by the original operation can still be
    /// returned. Operation exceptions that do not match the expected type propagate to the caller.
    /// </para>
    /// </remarks>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <typeparam name="TResult">Operation result type, discarded after completion.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<TException> ThrowsAnyAsync<TException, TResult>(
        ValueTask<TResult> operation,
        TimeSpan timeout,
        TimeProvider timeProvider,
        CancellationToken cancellationToken)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TException>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitAsync<TException, TResult>(operation, false), timeout, timeProvider, cancellationToken);
    }

    /// <summary>Asserts that the operation faults with exactly one of two explicitly allowed exception types.</summary>
    /// <typeparam name="TFirst">First expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <typeparam name="TSecond">Second expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <param name="operation">The original operation, without an added test timeout or cancellation wrapper.</param>
    /// <param name="timeout">Finite maximum time allowed for the assertion; zero checks completion immediately.</param>
    /// <param name="timeProvider">The clock used for the assertion timeout.</param>
    /// <param name="cancellationToken">The test runner's token; cancels the assertion wait without canceling the operation.</param>
    /// <returns>The exception produced by the operation, of exactly one of the two expected types.</returns>
    /// <remarks>
    /// Use this only when both specified failure types are valid outcomes of the scenario. Each listed type is matched exactly.
    /// Pass the original operation, not <c language="csharp">operation.WaitAsync(...)</c>, including through an intermediate
    /// variable or a <c language="csharp">ValueTask</c> wrapper. This helper cannot distinguish an operation failure from an
    /// exception produced by a wrapper; a wrapper exception matching either allowed type can incorrectly satisfy the assertion.
    /// The timeout and cancellation supplied to this method are outside exception matching and cannot satisfy it.
    /// Neither cancels the operation; the caller remains responsible for its cleanup. An unlisted exception propagates unchanged.
    /// </remarks>
    /// <exception cref="ArgumentNullException"><paramref name="operation" /> or <paramref name="timeProvider" /> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The timeout is negative or exceeds the supported timer range.</exception>
    /// <exception cref="OperationCanceledException">The assertion wait was canceled.</exception>
    /// <exception cref="AssertionException">An expected type is <see cref="Exception" />, or the operation completed successfully.</exception>
    /// <exception cref="TimeoutException">The assertion did not complete within the timeout.</exception>
    public static Task<Exception> ThrowsOneOfAsync<TFirst, TSecond>(Task operation, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TFirst : Exception
        where TSecond : Exception
    {
        ArgumentNullException.ThrowIfNull(operation);
        ArgumentNullException.ThrowIfNull(timeProvider);
        ExceptionTypeGuard.Validate<TFirst>();
        ExceptionTypeGuard.Validate<TSecond>();
        ValidateTimeout(timeout);
        return WaitAsync(AwaitOneOfAsync<TFirst, TSecond>(operation), timeout, timeProvider, cancellationToken);
    }

    private static async Task<TException> WaitAsync<TException>(Task<TException> assertion, TimeSpan timeout, TimeProvider timeProvider, CancellationToken cancellationToken)
        where TException : Exception
    {
        try
        {
            return await assertion.WaitAsync(timeout, timeProvider, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            // The guard may stop waiting before this owned task faults. Observe that late fault even after test cancellation.
            _ = assertion.ContinueWith(
                static failed => _ = failed.Exception,
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
            throw;
        }
    }

    private static void ValidateTimeout(TimeSpan timeout)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(timeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(timeout.TotalMilliseconds, uint.MaxValue - 1d);
    }

    private static async Task<TException> AwaitAsync<TException>(Task operation, bool exactType)
        where TException : Exception
    {
        try
        {
            await operation.ConfigureAwait(false);
        }
        catch (TException thrown) when (!exactType || thrown.GetType() == typeof(TException))
        {
            return thrown;
        }

        throw Missing<TException>();
    }

    private static async Task<TException> AwaitAsync<TException>(ValueTask operation, bool exactType)
        where TException : Exception
    {
        try
        {
            await operation.ConfigureAwait(false);
        }
        catch (TException thrown) when (!exactType || thrown.GetType() == typeof(TException))
        {
            return thrown;
        }

        throw Missing<TException>();
    }

    private static async Task<TException> AwaitAsync<TException, TResult>(ValueTask<TResult> operation, bool exactType)
        where TException : Exception
    {
        try
        {
            _ = await operation.ConfigureAwait(false);
        }
        catch (TException thrown) when (!exactType || thrown.GetType() == typeof(TException))
        {
            return thrown;
        }

        throw Missing<TException>();
    }

    private static async Task<Exception> AwaitOneOfAsync<TFirst, TSecond>(Task operation)
        where TFirst : Exception
        where TSecond : Exception
    {
        try
        {
            await operation.ConfigureAwait(false);
        }
        catch (TFirst thrown) when (thrown.GetType() == typeof(TFirst))
        {
            return thrown;
        }
        catch (TSecond thrown) when (thrown.GetType() == typeof(TSecond))
        {
            return thrown;
        }

        throw new AssertionException($"Expected {typeof(TFirst).FullName} or {typeof(TSecond).FullName} to be thrown, but the operation completed successfully.");
    }

    private static AssertionException Missing<TException>()
        where TException : Exception => new(NodeMissingExceptionMessage.For<TException>());
}
