using System;
using Squirix.Testing;
using TUnit.Assertions.Exceptions;

namespace Squirix.TestKit;

/// <summary>Creates closure-free assertions for synchronous exceptions.</summary>
public static class ExceptionAssert
{
    /// <summary>Creates an assertion for the exact expected exception type.</summary>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <returns>An exception assertion.</returns>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />.</exception>
    public static ExceptionExpectation<TException> For<TException>()
        where TException : Exception
    {
        ExceptionTypeGuard.Validate<TException>();
        return default;
    }
}
