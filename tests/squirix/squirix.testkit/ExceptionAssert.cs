using System;

namespace Squirix.TestKit;

/// <summary>Creates closure-free assertions for synchronous exceptions.</summary>
public static class ExceptionAssert
{
    /// <summary>Creates an assertion for the exact expected exception type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <returns>An exception assertion.</returns>
    public static ExceptionExpectation<TException> For<TException>()
        where TException : Exception => default;
}
