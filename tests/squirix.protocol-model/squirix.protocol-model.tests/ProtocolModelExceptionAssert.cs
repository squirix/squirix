using System;

namespace Squirix.ProtocolModel.Tests;

/// <summary>Creates closure-free assertions for synchronous exceptions.</summary>
public static class ProtocolModelExceptionAssert
{
    /// <summary>Creates an assertion for the exact expected exception type.</summary>
    /// <typeparam name="TException">Expected exception type.</typeparam>
    /// <returns>An exception assertion.</returns>
    public static ProtocolModelExceptionExpectation<TException> For<TException>()
        where TException : Exception => default;
}
