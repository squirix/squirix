using System;
using System.Threading.Tasks;
using Xunit.Sdk;

namespace Squirix.Server.TestKit;

/// <summary>Exception assertions for asynchronous operations that are already in flight.</summary>
/// <remarks>
///     <para>
///     These helpers accept the awaitable itself instead of a delegate, so call sites do not allocate the
///     display class that <c>Assert.ThrowsAsync</c> requires for its captured state.
///     </para>
///     <para>
///     The operation starts before the helper is entered, so only faults captured by the awaitable are
///     observed. Assertions on operations that throw synchronously (for example argument validation in a
///     non-async method body) must keep using <c>Assert.ThrowsAsync</c>.
///     </para>
/// </remarks>
public static class NodeAsyncAssert
{
    /// <summary>Awaits an in-flight operation and asserts it faults with <typeparamref name="TException" /> or a derived type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAnyAsync<TException>(Task operation)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(operation);
        return AwaitAsync<TException>(operation, false);
    }

    /// <summary>Awaits an in-flight operation and asserts it faults with <typeparamref name="TException" /> or a derived type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAnyAsync<TException>(ValueTask operation)
        where TException : Exception => AwaitAsync<TException>(operation, false);

    /// <summary>Awaits an in-flight operation and asserts it faults with <typeparamref name="TException" /> or a derived type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <typeparam name="TResult">Operation result type, discarded when the operation completes successfully.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAnyAsync<TException, TResult>(ValueTask<TResult> operation)
        where TException : Exception => AwaitAsync<TException, TResult>(operation, false);

    /// <summary>Awaits an in-flight operation and asserts it faults with exactly <typeparamref name="TException" />.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAsync<TException>(Task operation)
        where TException : Exception
    {
        ArgumentNullException.ThrowIfNull(operation);
        return AwaitAsync<TException>(operation, true);
    }

    /// <summary>Awaits an in-flight operation and asserts it faults with exactly <typeparamref name="TException" />.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAsync<TException>(ValueTask operation)
        where TException : Exception => AwaitAsync<TException>(operation, true);

    /// <summary>Awaits an in-flight operation and asserts it faults with exactly <typeparamref name="TException" />.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <typeparam name="TResult">Operation result type, discarded when the operation completes successfully.</typeparam>
    /// <param name="operation">The in-flight operation expected to fault.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public static Task<TException> ThrowsAsync<TException, TResult>(ValueTask<TResult> operation)
        where TException : Exception => AwaitAsync<TException, TResult>(operation, true);

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

    private static XunitException Missing<TException>()
        where TException : Exception => new(MissingCache<TException>.Message);

    private static class MissingCache<TException>
        where TException : Exception
    {
        internal static readonly string Message =
            $"Expected {typeof(TException).FullName} to be thrown, but the operation completed successfully.";
    }
}
