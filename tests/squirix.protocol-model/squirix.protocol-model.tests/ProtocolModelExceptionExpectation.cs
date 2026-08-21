using System;
using Xunit.Sdk;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Closure-free assertion for a synchronous exception.</summary>
/// <typeparam name="TException">Expected exception type.</typeparam>
public readonly record struct ProtocolModelExceptionExpectation<TException>
    where TException : Exception
{
    /// <summary>Invokes a capture-free operation and asserts it throws exactly <typeparamref name="TException" />.</summary>
    /// <param name="operation">Operation expected to throw.</param>
    /// <returns>The observed exception.</returns>
    /// <exception cref="ArgumentNullException">Thrown when <paramref name="operation" /> is <see langword="null" />.</exception>
    /// <exception cref="XunitException">Thrown when the operation completes successfully or throws an unexpected exception type.</exception>
    public TException Throws(Action operation)
    {
        ArgumentNullException.ThrowIfNull(operation);
        try
        {
            operation();
        }
        catch (Exception thrown) when (thrown is TException exact && exact.GetType() == typeof(TException))
        {
            return exact;
        }
        catch (Exception thrown)
        {
            throw new XunitException($"Expected {typeof(TException)} but observed {thrown.GetType()}.");
        }

        throw new XunitException($"Expected {typeof(TException)} but no matching exception was thrown.");
    }
}
