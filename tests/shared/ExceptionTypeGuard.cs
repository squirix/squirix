using System;
using TUnit.Assertions.Exceptions;

namespace Squirix.Testing;

/// <summary>Rejects exception assertions that accept the root exception type.</summary>
internal static class ExceptionTypeGuard
{
    internal static void Validate<TException>()
        where TException : Exception
    {
        if (typeof(TException) == typeof(Exception))
            throw new AssertionException("System.Exception cannot be used as the expected type. Specify a more specific exception type.");
    }
}
