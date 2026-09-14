using System;

namespace Squirix.TestKit;

/// <summary>Shared cached assertion text when an expected exception is not thrown.</summary>
internal static class MissingExceptionMessage
{
    internal static string For<TException>()
        where TException : Exception => Cache<TException>.Text;

    private static class Cache<TException>
        where TException : Exception
    {
        internal static readonly string Text = $"Expected {typeof(TException).FullName} to be thrown, but the operation completed successfully.";
    }
}
