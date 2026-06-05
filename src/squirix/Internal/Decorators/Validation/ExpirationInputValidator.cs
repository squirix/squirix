using System;

namespace Squirix.Internal.Decorators.Validation;

/// <summary>
/// Validates expiration arguments where a strictly positive duration is required (for example touch operations).
/// </summary>
internal static class ExpirationInputValidator
{
    /// <summary>
    /// Ensures <paramref name="expiration" /> is greater than zero.
    /// </summary>
    /// <param name="expiration">The expiration to validate.</param>
    /// <param name="parameterName">The caller parameter name for exceptions.</param>
    public static void ValidateRequiredPositive(TimeSpan expiration, string parameterName)
    {
        if (expiration <= TimeSpan.Zero)
            throw new ArgumentOutOfRangeException(parameterName, expiration, "expiration must be greater than zero.");
    }
}
