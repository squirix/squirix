using System;

namespace Squirix.Server.TestKit;

/// <summary>Creates closure-free assertions for synchronous node exceptions.</summary>
public static class NodeExceptionAssert
{
    /// <summary>Creates an assertion for the exact expected exception type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <returns>An exception assertion.</returns>
    public static NodeExceptionExpectation<TException> For<TException>()
        where TException : Exception => default;
}
