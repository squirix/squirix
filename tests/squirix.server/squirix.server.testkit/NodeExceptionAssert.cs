using System;
using Squirix.Testing;
using TUnit.Assertions.Exceptions;

namespace Squirix.Server.TestKit;

/// <summary>Creates closure-free assertions for synchronous node exceptions.</summary>
public static class NodeExceptionAssert
{
    /// <summary>Creates an assertion for the exact expected exception type.</summary>
    /// <typeparam name="TException">Expected exception type; <see cref="Exception" /> itself is not permitted.</typeparam>
    /// <returns>An exception assertion.</returns>
    /// <exception cref="AssertionException">The requested exception type is <see cref="Exception" />.</exception>
    public static NodeExceptionExpectation<TException> For<TException>()
        where TException : Exception
    {
        ExceptionTypeGuard.Validate<TException>();
        return default;
    }
}
