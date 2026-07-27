using System;
using Xunit.Sdk;

namespace Squirix.Server.TestKit;

/// <summary>Closure-free assertion for a synchronous node exception.</summary>
/// <typeparam name="TException">Expected exception type.</typeparam>
public readonly record struct NodeExceptionExpectation<TException>
    where TException : Exception
{
    private static readonly string MissingMessage = $"Expected {typeof(TException).FullName} to be thrown, but the operation completed successfully.";

    /// <summary>Invokes an operation with one state value and asserts it throws exactly <typeparamref name="TException" />.</summary>
    /// <typeparam name="TState">Operation state type.</typeparam>
    /// <param name="state">State passed to <paramref name="operation" />.</param>
    /// <param name="operation">Operation expected to throw.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public TException Throws<TState>(TState state, Action<TState> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            operation(state);
        }
        catch (TException thrown) when (thrown.GetType() == typeof(TException))
        {
            return thrown;
        }

        throw Missing();
    }

    /// <summary>Invokes an operation with two state values and asserts it throws exactly <typeparamref name="TException" />.</summary>
    /// <typeparam name="TState1">First operation state type.</typeparam>
    /// <typeparam name="TState2">Second operation state type.</typeparam>
    /// <param name="state1">First state value.</param>
    /// <param name="state2">Second state value.</param>
    /// <param name="operation">Operation expected to throw.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public TException Throws<TState1, TState2>(TState1 state1, TState2 state2, Action<TState1, TState2> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            operation(state1, state2);
        }
        catch (TException thrown) when (thrown.GetType() == typeof(TException))
        {
            return thrown;
        }

        throw Missing();
    }

    /// <summary>Invokes an operation with one state value and asserts it throws <typeparamref name="TException" /> or a derived type.</summary>
    /// <typeparam name="TState">Operation state type.</typeparam>
    /// <param name="state">State passed to <paramref name="operation" />.</param>
    /// <param name="operation">Operation expected to throw.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully.</exception>
    public TException ThrowsAny<TState>(TState state, Action<TState> operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            operation(state);
        }
        catch (TException thrown)
        {
            return thrown;
        }

        throw Missing();
    }

    private static XunitException Missing() => new(MissingMessage);
}
